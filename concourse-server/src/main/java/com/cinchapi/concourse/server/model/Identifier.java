/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.util.Comparator;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;

/**
 * An {@link Identifier} is an abstraction for an 8 byte long that represents
 * the canonical identifier for a normalized {@link Record}. The pool of
 * possible keys ranges from 0 to 2^64 - 1 inclusive.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class Identifier implements Byteable, Comparable<Identifier> {

    /**
     * The total number of bytes used to encode a {@link Identifier}.
     */
    public static final int SIZE = 8;

    /**
     * Return the {@link Identifier} encoded in {@code bytes} so long as those
     * bytes adhere to the format specified by the {@link #getBytes()} method.
     * 
     * @param bytes
     * @return the {@link Identifier}
     */
    public static Identifier fromByteBuffer(ByteBuffer bytes) {
        long value = bytes.getLong();
        return new Identifier(value);
    }

    /**
     * Return a {@link Identifier} that is backed by {@code data}.
     * 
     * @param value
     * @return the {@link Identifier}
     */
    public static Identifier of(long value) {
        return new Identifier(value);
    }

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    private transient ByteBuffer bytes;

    /**
     * The underlying data that represents this {@link Identifier}.
     */
    private final long value;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private Identifier(long data) {
        this.value = data;
        this.bytes = null;
    }

    /**
     * Compares keys such that they are sorted in ascending order.
     */
    @Override
    public int compareTo(Identifier other) {
        return UnsignedLongs.compare(value, other.value);
    }

    @Override
    public void copyTo(ByteSink sink) {
        sink.putLong(value);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Identifier) {
            final Identifier other = (Identifier) obj;
            return Longs.compare(value, other.value) == 0;
        }
        return false;
    }

    /**
     * Return a byte buffer that represents this {@link Identifier} with the
     * following
     * order:
     * <ol>
     * <li><strong>value</strong> - position 0</li>
     * </ol>
     * 
     * @return the ByteBuffer representation
     */
    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = Byteable.super.getBytes();
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(value);
    }

    /**
     * Return the long representation of this {@link Identifier}.
     * 
     * @return the long value
     */
    public long longValue() {
        return value;
    }

    @Override
    public int size() {
        return SIZE;
    }

    @Override
    public String toString() {
        return UnsignedLongs.toString(value);
    }

    /**
     * A {@link Comparator} that is used to sort {@link Identifier} objects.
     * 
     * @author Jeff Nelson
     */
    public static enum Sorter implements Comparator<Identifier> {
        INSTANCE;

        @Override
        public int compare(Identifier o1, Identifier o2) {
            return o1.compareTo(o2);
        }

    }

}
