/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.io.ByteBufferInputStream;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.util.Serializables;

/**
 * A wrapper around a {@link com.google.common.hash.BloomFilter} with methods
 * that make it easier to add one or more {@link Byteable} objects at a time
 * while also abstracting away the notion of funnels, etc.
 * </p>
 * 
 * @author Jeff Nelson
 */
@NotThreadSafe
public class BloomFilter implements Byteable {

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
     * Load an existing {@link BloomFilter} from the {@code bytes}.
     * 
     * @param bytes
     * @return the loaded {@link BloomFilter}
     */
    public static BloomFilter load(ByteBuffer bytes) {
        return new BloomFilter(bytes);
    }

    /**
     * Return the BloomFilter that is stored on disk in {@code file}.
     * 
     * @param file
     * @return the BloomFilter
     * @deprecated use {@link #load(ByteBuffer)} instead
     */
    @Deprecated
    public static BloomFilter open(Path file) {
        return load(FileSystem.readBytes(file.toString()));
    }

    /**
     * Return the BloomFilter that is stored on disk in {@code file}.
     * 
     * @param file
     * @return the BloomFilter
     * @deprecated use {@link #load(ByteBuffer)} instead
     */
    @Deprecated
    public static BloomFilter open(String file) {
        return open(Paths.get(file));
    }

    /**
     * The wrapped bloom filter. This is where the data is actually stored.
     */
    private final com.google.common.hash.BloomFilter<Composite> source;

    /**
     * Track if this {@link BloomFilter} was upgraded when being
     * {@link BloomFilter(ByteBuffer) loaded}.
     */
    private final boolean upgraded;

    /**
     * Construct a new instance.
     * 
     * @param bytes
     */
    @SuppressWarnings({ "unchecked" })
    private BloomFilter(ByteBuffer bytes) {
        try {
            final AtomicBoolean upgraded = new AtomicBoolean(false);
            ObjectInput input = new ObjectInputStream(
                    new BufferedInputStream(new ByteBufferInputStream(bytes))) {

                // In v0.3.0 the ByteableFunnel class was moved to a different
                // package, so we must translate any old data that exists.
                @Override
                protected ObjectStreamClass readClassDescriptor()
                        throws IOException, ClassNotFoundException {
                    ObjectStreamClass read = super.readClassDescriptor();
                    if(read.getName().equals(
                            "com.cinchapi.concourse.server.storage.ByteableFunnel")) {
                        upgraded.set(true);
                        return ObjectStreamClass.lookup(ByteableFunnel.class);
                    }
                    return read;
                }

            };
            this.source = (com.google.common.hash.BloomFilter<Composite>) input
                    .readObject();
            this.upgraded = upgraded.get();
            input.close();
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        catch (ClassNotFoundException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

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
        this.upgraded = false;
    }

    @Override
    public void copyTo(ByteSink sink) {
        ByteBuffer bytes = getBytes();
        sink.put(bytes);
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
    public ByteBuffer getBytes() {
        return Serializables.getBytes(source);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    /**
     * Return {@code true} if this {@link BloomFilter} was upgraded and should
     * have its {@link #getBytes() bytes} rewritten to any underlying data
     * store.
     * 
     * @return {@code true} if this {@link BloomFilter} was upgraded
     */
    public boolean isUpgraded() {
        return upgraded;
    }

    /**
     * Return true if the {@code composite} <strong>might</strong> have been put
     * in this filter or false if this is definitely not the case.
     * 
     * @param composite
     * @return {@code true} if {@code composite} might exist
     */
    public boolean mightContain(Composite composite) {
        return source.mightContain(composite);
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
     * Puts the {@link composite} item into this BloomFilter such that
     * subsequent invocations of {@link #mightContain(Composite)}
     * with the same {@link Composite} will always return true.
     * 
     * @param composite
     * @return {@code true} if the filter's bits changed as a result of this
     *         operation. If the bits changed, this is definitely the first time
     *         {@code byteables} have been added to the filter. If the bits
     *         haven't changed, this might be the first time they have been
     *         added. Note that put(t) always returns the opposite result to
     *         what mightContain(t) would have returned at the time it is
     *         called.
     */
    public boolean put(Composite composite) {
        return source.put(composite);
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
     * support for caching to ensure that subsequent invocations of
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
    public int size() {
        return getBytes().capacity();
    }

    /**
     * Return the underlying source for this {@link BloomFilter}.
     * 
     * @return the source
     */
    com.google.common.hash.BloomFilter<Composite> source() {
        return source;
    }

}
