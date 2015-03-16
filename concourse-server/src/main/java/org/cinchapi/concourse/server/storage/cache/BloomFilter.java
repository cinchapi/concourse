/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.cache;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Composite;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Serializables;
import org.cinchapi.concourse.server.io.Syncable;
import org.cinchapi.vendor.jsr166e.StampedLock;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A wrapper around a {@link com.google.common.hash.BloomFilter} with methods to
 * make it easier to add one or more {@link Byteable} objects to the filter at a
 * time while abstracting away the notion of funnels, etc.
 * </p>
 * 
 * @author jnelson
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
     * {@link BloomFilter#create(com.google.common.hash.Funnel, int)})
     * <p>
     * 
     * @param expectedInsertions
     * @return the BloomFilter
     */
    public static BloomFilter create(int expectedInsertions) {
        return new BloomFilter(null, expectedInsertions);
    }

    /**
     * Create a new BloomFilter with enough capacity for
     * {@code expectedInsertions}.
     * <p>
     * Note that overflowing a BloomFilter with significantly more elements than
     * specified, will result in its saturation, and a sharp deterioration of
     * its false positive probability (source:
     * {@link BloomFilter#create(com.google.common.hash.Funnel, int)})
     * <p>
     * 
     * @param file
     * @param expectedInsertions
     * @return the BloomFilter
     */
    public static BloomFilter create(String file, int expectedInsertions) {
        return new BloomFilter(file, expectedInsertions);
    }

    /**
     * Return the BloomFilter that is stored on disk in {@code file}.
     * 
     * @param file
     * @return the BloomFilter
     */
    @SuppressWarnings({ "unchecked" })
    public static BloomFilter open(String file) {
        try {
            final AtomicBoolean upgrade = new AtomicBoolean(false);
            ObjectInput input = new ObjectInputStream(new BufferedInputStream(
                    new FileInputStream(FileSystem.openFile(file)))) {

                // In v0.3.0 the ByteableFunnel class was moved to a different
                // package, so we must translate any old data that exists.
                // TODO: remove this check if a post 0.3 version
                @Override
                protected ObjectStreamClass readClassDescriptor()
                        throws IOException, ClassNotFoundException {
                    ObjectStreamClass read = super.readClassDescriptor();
                    if(read.getName()
                            .equals("org.cinchapi.concourse.server.storage.ByteableFunnel")) {
                        upgrade.set(true);
                        return ObjectStreamClass.lookup(ByteableFunnel.class);
                    }
                    return read;
                }

            };
            BloomFilter filter = new BloomFilter(file,
                    (com.google.common.hash.BloomFilter<Composite>) input
                            .readObject());
            input.close();
            if(upgrade.get()) {
                filter.sync();
            }
            return filter;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * The file where the content is stored.
     */
    protected String file;

    /**
     * Lock used to ensure the object is ThreadSafe. This lock provides access
     * to a masterLock.readLock()() and masterLock.writeLock()().
     */
    private final StampedLock lock = new StampedLock();

    /**
     * The wrapped bloom filter. This is where the data is actually stored.
     */
    protected final com.google.common.hash.BloomFilter<Composite> source;

    /**
     * Construct a new instance.
     * 
     * @param file
     * @param source
     */
    protected BloomFilter(String file,
            com.google.common.hash.BloomFilter<Composite> source) {
        this.source = source;
        this.file = file;
    }

    /**
     * Construct a new instance.
     * 
     * @param expectedInsertions
     */
    protected BloomFilter(String file, int expectedInsertions) {
        this.source = com.google.common.hash.BloomFilter.create(
                ByteableFunnel.INSTANCE, expectedInsertions); // uses 3% false
                                                              // positive
                                                              // probability
        this.file = file;
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
    // NOTE: It seems counter intuitive, but we take a read lock in this
    // method instead of a write lock so that readers can concurrently use
    // the bloom filter in memory while the content is being written to
    // disk.
    public void sync() {
        Preconditions.checkState(file != null, "Cannot sync a "
                + "BloomFilter that does not have an associated file");
        FileChannel channel = FileSystem.getFileChannel(file);
        long stamp = tryOptimisticRead();
        Serializables.write(source, channel); // CON-164
        if(!validate(stamp)) {
            stamp = readLock();
            try {
                channel.position(0);
                Serializables.write(source, channel); // CON-164
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            finally {
                unlockRead(stamp);
            }
        }
        FileSystem.closeFileChannel(channel);
    }

    /**
     * Check the backing bloom filter to see if the composite might have been
     * added.
     * 
     * @param composite
     * @return {@code true} if the composite might exist
     */
    private boolean mightContain(Composite composite) {
        long stamp = tryOptimisticRead();
        boolean mightContain = source.mightContain(composite);
        if(!validate(stamp)) {
            stamp = readLock();
            try {
                mightContain = source.mightContain(composite);
            }
            finally {
                unlockRead(stamp);
            }
        }
        return mightContain;
    }

    /**
     * Place the {@code composite} in the backing bloom filter.
     * 
     * @param composite
     * @return {@code true} if the bits have changed as a result of the addition
     *         of the {@code composite}
     */
    private boolean put(Composite composite) {
        long stamp = writeLock();
        try {
            return source.put(composite);
        }
        finally {
            unlockWrite(stamp);
        }
    }

    /**
     * Grabs the write {@link StampedLock} {@code lock} and return the
     * {@code stamp}.
     * 
     * @return {@code stamp}
     */
    protected long writeLock() {
        return lock.writeLock();
    }

    /**
     * Release the write {@link StampedLock} {@code lock} for {@code stamp}.
     * 
     * @param {@code stamp}
     */
    protected void unlockWrite(long stamp) {
        lock.unlockWrite(stamp);
    }

    /**
     * Returns stamp using {@link #tryOptimisticRead() tryOptimisticRead()}
     * 
     * @return {@code stamp}
     */
    protected long tryOptimisticRead() {
        return lock.tryOptimisticRead();
    }

    /**
     * Checks whether the lock has not been exclusively acquired since issuance
     * of the given stamp using {@link #validate(long) validate(long stamp)}
     * method.
     * 
     * @param {@code stamp}
     * @return boolean
     */
    protected boolean validate(long stamp) {
        return lock.validate(stamp);
    }

    /**
     * Grabs the read {@link StampedLock} {@code lock} and return the
     * {@code stamp}.
     * 
     * @return {@code stamp}
     */
    protected long readLock() {
        return lock.readLock();
    }

    /**
     * Release the read {@link StampedLock} {@code lock} for {@code stamp}.
     * 
     * @param {@code stamp}
     */
    protected void unlockRead(long stamp) {
        lock.unlockRead(stamp);
    }

}
