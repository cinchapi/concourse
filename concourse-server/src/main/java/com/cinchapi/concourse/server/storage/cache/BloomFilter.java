/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage.cache;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Syncable;
import com.cinchapi.concourse.util.Serializables;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * A wrapper around a {@link com.google.common.hash.BloomFilter} with methods
 * that make it easier to add one or more {@link Byteable} objects at a time
 * while also abstracting away the notion of funnels, etc.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class BloomFilter implements Syncable {

    /**
     * Create a new BloomFilter with enough capacity for
     * {@code expectedInsertions} but cannot be synced to disk.
     * <p>
     * Note that overflowing a BloomFilter with significantly more elements than
     * specified, will result in its saturation, and a sharp deterioration of
     * its false positive probability (source:
     * {@link BloomFilter#reserve(com.google.common.hash.Funnel, int)})
     * <p>
     * 
     * @param expectedInsertions
     * @return the BloomFilter
     */
    public static BloomFilter create(int expectedInsertions) {
        return new BloomFilter(expectedInsertions);
    }

    /**
     * Return the BloomFilter that is stored on disk in {@code file}.
     * 
     * @param file
     * @return the BloomFilter
     */
    public static BloomFilter open(Path file) {
        return new BloomFilter(file);
    }

    /**
     * Return the BloomFilter that is stored on disk in {@code file}.
     * 
     * @param file
     * @return the BloomFilter
     */
    public static BloomFilter open(String file) {
        return open(Paths.get(file));
    }

    /**
     * Create a new BloomFilter with enough capacity for
     * {@code expectedInsertions}.
     * <p>
     * Note that overflowing a BloomFilter with significantly more elements than
     * specified, will result in its saturation, and a sharp deterioration of
     * its false positive probability (source:
     * {@link BloomFilter#reserve(com.google.common.hash.Funnel, int)})
     * <p>
     * 
     * @param file
     * @param expectedInsertions
     * @return the BloomFilter
     */
    public static BloomFilter reserve(Path file, int expectedInsertions) {
        BloomFilter filter = new BloomFilter(expectedInsertions);
        filter.intendedFile = file;
        return filter;
    }

    /**
     * Create a new BloomFilter with enough capacity for
     * {@code expectedInsertions}.
     * <p>
     * Note that overflowing a BloomFilter with significantly more elements than
     * specified, will result in its saturation, and a sharp deterioration of
     * its false positive probability (source:
     * {@link BloomFilter#reserve(com.google.common.hash.Funnel, int)})
     * <p>
     * 
     * @param file
     * @param expectedInsertions
     * @return the BloomFilter
     */
    public static BloomFilter reserve(String file, int expectedInsertions) {
        return reserve(file, expectedInsertions);
    }

    /**
     * The file where the {@link BloomFilter} was last {@link #sync(Path)
     * synced}. This value is {@code null} if the {@link BloomFilter} has never
     * been {@link #sync(Path) synced}
     */
    @Nullable
    private Path file;

    /**
     * A file passed in with the the legacy {@link #reserve(Path, int)} factory.
     * If this exist, {@link #sync()} to it if {@link #file} has not been set.
     */
    @Nullable
    private Path intendedFile;

    /**
     * Lock used to ensure the object is ThreadSafe. This lock provides access
     * to a masterLock.readLock()() and masterLock.writeLock()().
     */
    private final StampedLock lock = new StampedLock();

    /**
     * The wrapped bloom filter. This is where the data is actually stored.
     */
    private final com.google.common.hash.BloomFilter<Composite> source;

    /**
     * A flag that indicates if this BloomFilter instance does locking and is
     * therefore thread safe under concurrent access. This is configurable using
     * the {@link #enableThreadSafety()} and {@link #disableThreadSafety()}
     * methods.
     */
    private boolean threadSafe = true;

    /**
     * Construct a new instance.
     * 
     * @param expectedInsertions
     */
    private BloomFilter(int expectedInsertions) {
        this.source = com.google.common.hash.BloomFilter
                .create(ByteableFunnel.INSTANCE, expectedInsertions); // uses 3%
                                                                      // false
                                                                      // positive
                                                                      // probability
        this.file = null;
    }

    /**
     * Construct a new instance.
     * 
     * @param file
     */
    @SuppressWarnings({ "unchecked" })
    private BloomFilter(Path file) {
        this.file = file;
        try {
            String $file = file.toString();
            final AtomicBoolean upgrade = new AtomicBoolean(false);
            ObjectInput input = new ObjectInputStream(new BufferedInputStream(
                    new FileInputStream(FileSystem.openFile($file)))) {

                // In v0.3.0 the ByteableFunnel class was moved to a different
                // package, so we must translate any old data that exists.
                @Override
                protected ObjectStreamClass readClassDescriptor()
                        throws IOException, ClassNotFoundException {
                    ObjectStreamClass read = super.readClassDescriptor();
                    if(read.getName().equals(
                            "com.cinchapi.concourse.server.storage.ByteableFunnel")) {
                        upgrade.set(true);
                        return ObjectStreamClass.lookup(ByteableFunnel.class);
                    }
                    return read;
                }

            };
            this.source = (com.google.common.hash.BloomFilter<Composite>) input
                    .readObject();
            input.close();
            if(upgrade.get()) {
                sync();
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        catch (ClassNotFoundException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Turn off thread safety for this BloomFilter. Only do this when it is
     * certain that the bloom filter will not see any additional writes from
     * multiple concurrent threads.
     */
    public void disableThreadSafety() {
        threadSafe = false;
    }

    /**
     * Turn on thread safety for this BloomFilter.
     */
    public void enableThreadSafety() {
        threadSafe = true;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof BloomFilter) {
            BloomFilter other = (BloomFilter) obj;
            return source.equals(other.source);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    /**
     * Return true if an element made up of {@code byteables} might have been
     * put in this filter or false if this is definitely not the case.
     * 
     * @param byteables
     * @return {@code true} if {@code byteables} might exist
     */
    public boolean mightContain(Byteable... byteables) {
        Composite composite = Composite.create(byteables);
        return mightContain(composite);
    }

    /**
     * Return true if an element made up of a cached copy of the
     * {@code byteables} might have been put in this filter or false if this is
     * definitely not the case.
     * <p>
     * Since caching is involved, this method is more prone to false positives
     * than the {@link #mightContain(Byteable...)} alternative, but it will
     * never return false negatives as long as the bits were added with the
     * {@code #putCached(Byteable...)} method.
     * </p>
     * 
     * @param byteables
     * @return {@code true} if {@code byteables} might exist
     */
    public boolean mightContainCached(Byteable... byteables) {
        Composite composite = Composite.createCached(byteables);
        return mightContain(composite);
    }

    /**
     * <p>
     * <strong>Copied from {@link BloomFilter#put(Object)}.</strong>
     * </p>
     * Puts {@link byteables} into this BloomFilter as a single element.
     * Ensures that subsequent invocations of {@link #mightContain(Byteable...)}
     * with the same elements will always return true.
     * 
     * @param byteables
     * @return {@code true} if the filter's bits changed as a result of this
     *         operation. If the bits changed, this is definitely the first time
     *         {@code byteables} have been added to the filter. If the bits
     *         haven't changed, this might be the first time they have been
     *         added. Note that put(t) always returns the opposite result to
     *         what mightContain(t) would have returned at the time it is
     *         called.
     */
    public boolean put(Byteable... byteables) {
        return put(Composite.create(byteables));
    }

    /**
     * <p>
     * <strong>Copied from {@link BloomFilter#put(Object)}.</strong>
     * </p>
     * Puts {@link byteables} into this BloomFilter as a single element with
     * support for caching Ensures that subsequent invocations of
     * {@link #mightContainCached(Byteable...)} with the same elements will
     * always return true.
     * 
     * @param byteables
     * @return {@code true} if the filter's bits changed as a result of this
     *         operation. If the bits changed, this is definitely the first time
     *         {@code byteables} have been added to the filter. If the bits
     *         haven't changed, this might be the first time they have been
     *         added. Note that put(t) always returns the opposite result to
     *         what mightContain(t) would have returned at the time it is
     *         called.
     */
    public boolean putCached(Byteable... byteables) {
        return put(Composite.createCached(byteables));
    }

    @Override
    public void sync() {
        Preconditions.checkState(file != null || intendedFile != null,
                "Cannot sync because a file has not been specified");
        sync(MoreObjects.firstNonNull(file, intendedFile));
    }

    /**
     * Write the data to {@code file} and fsync to guarantee durability.
     * 
     * @param file
     */
    public void sync(Path file) {
        // NOTE: It seems counter intuitive, but we take a read lock in this
        // method instead of a write lock so that readers can concurrently use
        // the bloom filter in memory while the content is being written to
        // disk.
        FileChannel channel = FileSystem.getFileChannel(file.toString());
        try {
            long stamp = lock.tryOptimisticRead();
            Serializables.write(source, channel); // CON-164
            if(!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    channel.truncate(1);
                    channel.position(0);
                    Serializables.write(source, channel); // CON-164
                }
                catch (IOException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
                finally {
                    lock.unlockRead(stamp);
                }
            }
            this.file = file;
        }
        finally {
            FileSystem.closeFileChannel(channel);
        }
    }

    /**
     * Check the backing bloom filter to see if the composite might have been
     * added.
     * 
     * @param composite
     * @return {@code true} if the composite might exist
     */
    private boolean mightContain(Composite composite) {
        if(threadSafe) {
            long stamp = lock.tryOptimisticRead();
            boolean mightContain = source.mightContain(composite);
            if(!lock.validate(stamp)) {
                stamp = lock.readLock();
                try {
                    mightContain = source.mightContain(composite);
                }
                finally {
                    lock.unlockRead(stamp);
                }
            }
            return mightContain;
        }
        else {
            return source.mightContain(composite);
        }
    }

    /**
     * Place the {@code composite} in the backing bloom filter.
     * 
     * @param composite
     * @return {@code true} if the bits have changed as a result of the addition
     *         of the {@code composite}
     */
    private boolean put(Composite composite) {
        if(threadSafe) {
            long stamp = lock.writeLock();
            try {
                return source.put(composite);
            }
            finally {
                lock.unlockWrite(stamp);
            }
        }
        else {
            return source.put(composite);
        }
    }
    
    

}
