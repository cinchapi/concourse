/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.util.Comparator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;

/**
 * A PrimaryKey is an abstraction for an 8 byte long that represents the
 * canonical identifier for a normalized {@link Record}. The pool of possible
 * keys ranges from 0 to 2^64 1 inclusive.
 * 
 * @author jnelson
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
            copyToByteBuffer(bytes);
            bytes.flip();
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
    public void copyToByteBuffer(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    /**
     * A {@link Comparator} that is used to sort PrimaryKey objects.
     * 
     * @author jnelson
     */
    public static enum Sorter implements Comparator<PrimaryKey> {
        INSTANCE;

        @Override
        public int compare(PrimaryKey o1, PrimaryKey o2) {
            return o1.compareTo(o2);
        }

    }

}
