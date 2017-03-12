/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;

/**
 * A PrimaryKey is an abstraction for an 8 byte long that represents the
 * canonical identifier for a normalized {@link Record}. The pool of possible
 * keys ranges from 0 to 2^64 1 inclusive.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class PrimaryKey implements Byteable, Comparable<PrimaryKey> {

    /**
     * Return the PrimaryKey encoded in {@code bytes} so long as those bytes
     * adhere to the format specified by the {@link #getBytes()} method. This
     * method assumes that all the bytes in the {@code bytes} belong to the
     * PrimaryKey. In general, it is necessary to get the appropriate PrimaryKey
     * slice
     * from the parent ByteBuffer using
     * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the PrimaryKey
     */
    public static PrimaryKey fromByteBuffer(ByteBuffer bytes) {
        long data = bytes.getLong();
        return new PrimaryKey(data, bytes);
    }

    /**
     * Return a PrimaryKey that is backed by {@code data}.
     * 
     * @param data
     * @return the PrimaryKey
     */
    public static PrimaryKey wrap(long data) {
        return new PrimaryKey(data);
    }

    /**
     * The total number of bytes used to encode a PrimaryKey.
     */
    public static final int SIZE = 8;

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    private transient ByteBuffer bytes = null;

    /**
     * The underlying data that represents this PrimaryKey.
     */
    private final long data;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private PrimaryKey(long data) {
        this(data, null);
    }

    /**
     * Construct a new instance.
     * 
     * @param data
     * @param bytes
     */
    private PrimaryKey(long data, @Nullable ByteBuffer bytes) {
        this.data = data;
        this.bytes = bytes;
    }

    /**
     * Compares keys such that they are sorted in ascending order.
     */
    @Override
    public int compareTo(PrimaryKey other) {
        return UnsignedLongs.compare(data, other.data);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof PrimaryKey) {
            final PrimaryKey other = (PrimaryKey) obj;
            return Longs.compare(data, other.data) == 0;
        }
        return false;
    }

    /**
     * Return a byte buffer that represents this PrimaryKey with the following
     * order:
     * <ol>
     * <li><strong>data</strong> - position 0</li>
     * </ol>
     * 
     * @return the ByteBuffer representation
     */
    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = ByteBuffer.allocate(SIZE);
            copyTo(bytes);
            bytes.rewind();
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(data);
    }

    /**
     * Return the long representation of this PrimaryKey.
     * 
     * @return the long value
     */
    public long longValue() {
        return data;
    }

    @Override
    public int size() {
        return SIZE;
    }

    @Override
    public String toString() {
        return UnsignedLongs.toString(data);
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    /**
     * A {@link Comparator} that is used to sort PrimaryKey objects.
     * 
     * @author Jeff Nelson
     */
    public static enum Sorter implements Comparator<PrimaryKey> {
        INSTANCE;

        @Override
        public int compare(PrimaryKey o1, PrimaryKey o2) {
            return o1.compareTo(o2);
        }

    }

}
