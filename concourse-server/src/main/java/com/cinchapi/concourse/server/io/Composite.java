/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.io.ByteBuffers;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * A {@link Composite} is a single {@link Byteable} composed of other
 * {@link Byteable Byteables}. Equal {@link Byteable Byteables} composed in the
 * same order will always generate an equal {@link Composite}.
 * <p>
 * A {@link Composite} is used to construct keys with multiple parts.
 * </p>
 * <p>
 * There is no guarantee that the component {@link #parts()} of a
 * {@link Composite} cannot be retrieved or reconstructed from a
 * {@link Composite}.
 * </p>
 * <p>
 * The maximum {@link #size} of a {@link Composite} is {@link #MAX_SIZE};
 * however some may have a smaller size if the component parts are sufficiently
 * small.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class Composite implements Byteable {

    /**
     * Create a new {@link Composite}.
     * 
     * @param parts
     * @return the new {@link Composite}
     */
    public static Composite create(Byteable... parts) {
        if(parts.length == 1 && parts[0] instanceof Composite) {
            return (Composite) parts[0];
        }
        else {
            return new Composite(parts);
        }
    }

    /**
     * Create a Composite for the list of {@code objects} with support for
     * caching. Cached Composites are not guaranteed to perfectly match up with
     * the list of objects (because hash collisions can occur) so it is only
     * advisable to use this method of creation when precision is not a
     * requirement.
     * 
     * @param objects
     * @return the Composite
     */
    public static Composite createCached(Byteable... objects) {
        int hashCode = Arrays.hashCode(objects);
        Composite composite = CACHE.get(hashCode);
        if(composite == null) {
            composite = create(objects);
            CACHE.put(hashCode, composite);
        }
        return composite;
    }

    /**
     * Load an existing {@link Composite} from a {@link ByteBuffer}.
     * 
     * @param bytes
     * @return the loaded {@link Composite}
     */
    public static Composite load(ByteBuffer bytes) {
        return new Composite(bytes);
    }

    /**
     * The largest possible {@link #size()}.
     */
    public static final int MAX_SIZE = 32;

    /**
     * A cache of Composite. Each composite is associated with the cumulative
     * hashcode of all the things that went into the composite.
     */
    private final static Map<Integer, Composite> CACHE = Maps.newHashMap();

    /**
     * The composite bytes.
     */
    private final byte[] bytes;

    /**
     * The input parts. This is generally not available when the
     * {@link Composite} is {@link #load(ByteBuffer) loaded}.
     */
    private final Byteable[] parts;

    /**
     * Construct a new instance.
     * 
     * @param parts
     */
    private Composite(Byteable... parts) {
        this.parts = parts;
        int size = 0;
        for (Byteable part : parts) {
            size += part.getCanonicalLength() + 4;
        }
        RetrievableByteSink sink = size < MAX_SIZE
                ? new WrappedByteArraySink(size)
                : new HasherByteSink(Hashing.sha256().newHasher(size));
        int pos = 0;
        for (Byteable part : parts) {
            sink.putInt(pos);
            part.copyCanonicalBytesTo(sink);
            ++pos;
        }
        sink.flush();
        this.bytes = sink.retrieve();
    }

    /**
     * Load a new instance.
     * 
     * @param bytes
     */
    private Composite(ByteBuffer bytes) {
        this.bytes = ByteBuffers.getByteArray(bytes);
        this.parts = null;
    }

    @Override
    public void copyTo(ByteSink sink) {
        sink.put(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Composite) {
            Composite other = (Composite) obj;
            return Arrays.equals(bytes, other.bytes);
        }
        return false;
    }

    @Override
    public ByteBuffer getBytes() {
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Return the value of {@link #getBytes()} as a {@code byte[]}.
     * 
     * @return the {@link #getBytes() bytes}
     */
    public byte[] bytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    /**
     * Return {@code true} if {@link #parts()} will return a non-null value.
     * 
     * @return {@code true} if this {@link Composite} is aware of its individual
     *         parts
     */
    public boolean hasParts() {
        return parts != null;
    }

    /**
     * Return the individual {@link Byteable parts} that make up this
     * {@link Composite}, if they're available. If the parts are not available,
     * return {@code null}
     * 
     * @return the {@link Composite} parts
     */
    @Nullable
    public Byteable[] parts() {
        return parts;
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public String toString() {
        return Hashing.murmur3_128().hashBytes(getBytes()).toString();
    }

    /**
     * A {@link ByteSink} that records bytes in a {@link Hasher} and returns a
     * {@link ByteBuffer} containing the hashed bytes in the {@link #retrieve()}
     * method.
     *
     * @author Jeff Nelson
     */
    private static final class HasherByteSink extends RetrievableByteSink {

        /**
         * The underlying {@link Hasher}.
         */
        private final Hasher hasher;

        /**
         * Construct a new instance.
         * 
         * @param hasher
         */
        private HasherByteSink(Hasher hasher) {
            this.hasher = hasher;
        }

        @Override
        public long position() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteSink put(byte value) {
            hasher.putByte(value);
            return this;
        }

        @Override
        public ByteSink put(byte[] src) {
            hasher.putBytes(src);
            return this;
        }

        @Override
        public ByteSink put(ByteBuffer src) {
            hasher.putBytes(src);
            return this;
        }

        @Override
        public ByteSink putChar(char value) {
            hasher.putChar(value);
            return this;
        }

        @Override
        public ByteSink putDouble(double value) {
            hasher.putDouble(value);
            return this;
        }

        @Override
        public ByteSink putFloat(float value) {
            hasher.putFloat(value);
            return this;
        }

        @Override
        public ByteSink putInt(int value) {
            hasher.putInt(value);
            return this;
        }

        @Override
        public ByteSink putLong(long value) {
            hasher.putLong(value);
            return this;
        }

        @Override
        public ByteSink putShort(short value) {
            hasher.putShort(value);
            return this;
        }

        @Override
        protected byte[] retrieve() {
            return hasher.hash().asBytes();
        }

    }

    /**
     * A {@link ByteSink} that allows a {@link ByteBuffer} containing all of its
     * content to be {@link #retrieve() retrieved}
     *
     * @author Jeff Nelson
     */
    private static abstract class RetrievableByteSink implements ByteSink {

        /**
         * Return all the bytes written to this {@link ByteSink sink} as a
         * {@code byte[]}.
         * 
         * @return a {@code byte} with the content of this {@link ByteSink
         *         sink}
         */
        protected abstract byte[] retrieve();
    }

    /**
     * A {@link ByteSink#to(ByteBuffer) ByteSink that writes to a ByteBuffer},
     * but is wrapped so that the underlying source is managed, yet accessible
     * via {@link #retrieve()}.
     *
     * @author Jeff Nelson
     */
    private static final class WrappedByteArraySink
            extends RetrievableByteSink {

        /**
         * The source to which {@link #sink} writes.
         */
        private final byte[] bytes;

        /**
         * The wrapped {@link ByteSink}.
         */
        private final ByteSink sink;

        /**
         * Construct a new instance.
         * 
         * @param size
         */
        private WrappedByteArraySink(int size) {
            this.bytes = new byte[size];
            this.sink = ByteSink.to(bytes);
        }

        @Override
        public long position() {
            return sink.position();
        }

        @Override
        public ByteSink put(byte value) {
            sink.put(value);
            return this;
        }

        @Override
        public ByteSink put(byte[] src) {
            sink.put(src);
            return this;
        }

        @Override
        public ByteSink put(ByteBuffer src) {
            sink.put(src);
            return this;
        }

        @Override
        public ByteSink putChar(char value) {
            sink.putChar(value);
            return this;
        }

        @Override
        public ByteSink putDouble(double value) {
            sink.putDouble(value);
            return this;
        }

        @Override
        public ByteSink putFloat(float value) {
            sink.putFloat(value);
            return this;
        }

        @Override
        public ByteSink putInt(int value) {
            sink.putInt(value);
            return this;
        }

        @Override
        public ByteSink putLong(long value) {
            sink.putLong(value);
            return this;
        }

        @Override
        public ByteSink putShort(short value) {
            sink.putShort(value);
            return this;
        }

        @Override
        protected byte[] retrieve() {
            return bytes;
        }
    }

}
