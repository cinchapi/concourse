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
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Preconditions;

/**
 * A Position is an abstraction for the association between a
 * relative location and a {@link PrimaryKey} that is used in a
 * {@link SearchRecord} to specify the location of a term in a record.
 * 
 * @author jnelson
 */
@Immutable
public final class Position implements Byteable, Comparable<Position> {

    /**
     * Return the Position encoded in {@code bytes} so long as those bytes
     * adhere to the format specified by the {@link #getBytes()} method. This
     * method assumes that all the bytes in the {@code bytes} belong to the
     * Position. In general, it is necessary to get the appropriate Position
     * slice from the parent ByteBuffer using
     * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the Position
     */
    public static Position fromByteBuffer(ByteBuffer bytes) {
        PrimaryKey primaryKey = PrimaryKey.fromByteBuffer(ByteBuffers.get(
                bytes, PrimaryKey.SIZE));
        int index = bytes.getInt();
        return new Position(primaryKey, index);
    }

    /**
     * Return a Position that is backed by {@code primaryKey} and {@code index}.
     * 
     * @param primaryKey
     * @param index
     * @return the Position
     */
    public static Position wrap(PrimaryKey primaryKey, int index) {
        return new Position(primaryKey, index);
    }

    /**
     * The total number of bytes used to store each Position
     */
    public static final int SIZE = PrimaryKey.SIZE + 4; // index

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    private transient ByteBuffer bytes;

    /**
     * The index that this Position represents.
     */
    private final int index;

    /**
     * The PrimaryKey of the record that this Position represents.
     */
    private final PrimaryKey primaryKey;

    /**
     * Construct a new instance.
     * 
     * @param primaryKey
     * @param index
     */
    private Position(PrimaryKey primaryKey, int index) {
        this(primaryKey, index, null);
    }

    /**
     * Construct a new instance.
     * 
     * @param primaryKey
     * @param index
     * @param bytes;
     */
    private Position(PrimaryKey primaryKey, int index,
            @Nullable ByteBuffer bytes) {
        Preconditions
                .checkArgument(index >= 0, "Cannot have an negative index");
        this.primaryKey = primaryKey;
        this.index = index;
        this.bytes = bytes;
    }

    @Override
    public int compareTo(Position other) {
        int comparison;
        return (comparison = primaryKey.compareTo(other.primaryKey)) != 0 ? comparison
                : Integer.compare(index, other.index);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Position) {
            Position other = (Position) obj;
            return primaryKey.equals(other.primaryKey) && index == other.index;
        }
        return false;
    }

    /**
     * Return a byte buffer that represents this Value with the following order:
     * <ol>
     * <li><strong>primaryKey</strong> - position 0</li>
     * <li><strong>index</strong> - position 8</li>
     * </ol>
     * 
     * @return the ByteBuffer representation
     */
    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = ByteBuffer.allocate(size());
<<<<<<< HEAD
            copyTo(bytes);
=======
            copyToByteBuffer(bytes);
>>>>>>> de8748264fd8f0370664c027005cdaf90ba95252
            bytes.rewind();
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    /**
     * Return the associated {@code index}.
     * 
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    /**
     * Return the associated {@code primaryKey}.
     * 
     * @return the primaryKey
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, index);
    }

    @Override
    public int size() {
        return SIZE;
    }

    @Override
    public String toString() {
        return "Position " + index + " in Record " + primaryKey;
    }

    @Override
<<<<<<< HEAD
    public void copyTo(ByteBuffer buffer) {
=======
    public void copyToByteBuffer(ByteBuffer buffer) {
>>>>>>> de8748264fd8f0370664c027005cdaf90ba95252
        // NOTE: Storing the index as an int instead of some size aware
        // variable length is probably overkill since most indexes will be
        // smaller than Byte.MAX_SIZE or Short.MAX_SIZE, but having variable
        // size indexes means that the size of the entire Position (as an
        // int) must be stored before the Position for proper
        // deserialization. By storing the index as an int, the size of each
        // Position is constant so we won't need to store the overall size
        // prior to the Position to deserialize it, which is actually more
        // space efficient.
<<<<<<< HEAD
        primaryKey.copyTo(buffer);
=======
        primaryKey.copyToByteBuffer(buffer);
>>>>>>> de8748264fd8f0370664c027005cdaf90ba95252
        buffer.putInt(index);
    }

}
