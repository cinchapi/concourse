/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.google.common.base.Preconditions;

/**
 * A Position is an abstraction for the association between a
 * relative location and a {@link Identifier} that is used in a
 * {@link CorpusRecord} to specify the location of a term in a record.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class Position implements Byteable, Comparable<Position> {

    /**
     * The total number of bytes used to store each Position
     */
    public static final int SIZE = Identifier.SIZE + 4; // index

    /**
     * Return the Position encoded in {@code bytes} so long as those bytes
     * adhere to the format specified by the {@link #getBytes()} method.
     * 
     * @param bytes
     * @return the Position
     */
    public static Position fromByteBuffer(ByteBuffer bytes) {
        Identifier identifier = Identifier.fromByteBuffer(bytes);
        int index = bytes.getInt();
        return new Position(identifier, index);
    }

    /**
     * Return a Position that is backed by {@code primaryKey} and {@code index}.
     * 
     * @param identifier
     * @param index
     * @return the Position
     */
    public static Position of(Identifier identifier, int index) {
        return new Position(identifier, index);
    }

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
    private final Identifier identifier;

    /**
     * Construct a new instance.
     * 
     * @param identifier
     * @param index
     */
    private Position(Identifier identifier, int index) {
        Preconditions.checkArgument(index >= 0,
                "Cannot have an negative index");
        this.identifier = identifier;
        this.index = index;
        this.bytes = null;
    }

    @Override
    public int compareTo(Position other) {
        int comparison;
        return (comparison = identifier.compareTo(other.identifier)) != 0
                ? comparison
                : Integer.compare(index, other.index);
    }

    @Override
    public void copyTo(ByteSink sink) {
        // NOTE: Storing the index as an int instead of some size aware
        // variable length is probably overkill since most indexes will be
        // smaller than Byte.MAX_SIZE or Short.MAX_SIZE, but having variable
        // size indexes means that the size of the entire Position (as an
        // int) must be stored before the Position for proper
        // deserialization. By storing the index as an int, the size of each
        // Position is constant so we won't need to store the overall size
        // prior to the Position to deserialize it, which is actually more
        // space efficient.
        identifier.copyTo(sink);
        sink.putInt(index);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Position) {
            Position other = (Position) obj;
            return identifier.equals(other.identifier) && index == other.index;
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
            bytes = Byteable.super.getBytes();
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
     * Return the associated {@link #identifier}.
     * 
     * @return the primaryKey
     */
    public Identifier getIdentifier() {
        return identifier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, index);
    }

    @Override
    public int size() {
        return SIZE;
    }

    @Override
    public String toString() {
        return "Position " + index + " in Record " + identifier;
    }

}
