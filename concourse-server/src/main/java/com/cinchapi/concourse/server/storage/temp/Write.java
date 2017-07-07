/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.temp;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.Versioned;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;

/**
 * A Write is a {@link Byteable} and {@link Versioned} container that serves as
 * a temporary representation of a revision before it is permanently stored and
 * indexed.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class Write implements Byteable, Versioned {

    /**
     * Return a storable Write that represents a revision to ADD {@code key} as
     * {@code value} to {@code record}.
     * 
     * @param key
     * @param value
     * @param record
     * @return the Write
     */
    public static Write add(String key, TObject value, long record) {
        return new Write(Action.ADD, Text.wrapCached(key), Value.wrap(value),
                PrimaryKey.wrap(record), Time.now());
    }

    /**
     * Return the Write encoded in {@code bytes} so long as those bytes adhere
     * to the format specified by the {@link #getBytes()} method. This method
     * assumes that all the bytes in the {@code bytes} belong to the Value. In
     * general, it is necessary to get the appropriate Write slice from the
     * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the Value
     */
    public static Write fromByteBuffer(ByteBuffer bytes) {
        int keySize = bytes.getInt();
        Action type = Action.values()[bytes.get()];
        long version = bytes.getLong();
        PrimaryKey record = PrimaryKey.fromByteBuffer(ByteBuffers.get(bytes,
                PrimaryKey.SIZE));
        Text key = Text.fromByteBuffer(ByteBuffers.get(bytes, keySize));
        Value value = Value.fromByteBuffer(ByteBuffers.get(bytes,
                bytes.remaining()));
        return new Write(type, key, value, record, version);
    }

    /**
     * Return a notStorable Write that represents any revision involving
     * {@code key} as {@code value} in {@code record}.
     * 
     * @param key
     * @param value
     * @param record
     * @return the Write
     */
    public static Write notStorable(String key, TObject value, long record) {
        return new Write(Action.COMPARE, Text.wrapCached(key),
                Value.wrap(value), PrimaryKey.wrap(record), NO_VERSION);
    }

    /**
     * Return a storable Write that represents a revision to REMOVE {@code key}
     * as {@code value} from {@code record}.
     * 
     * @param key
     * @param value
     * @param record
     * @return the Write
     */
    public static Write remove(String key, TObject value, long record) {
        return new Write(Action.REMOVE, Text.wrapCached(key),
                Value.wrap(value), PrimaryKey.wrap(record), Time.now());
    }

    /**
     * The minimum number of bytes needed to encode every Write.
     */
    private static final int CONSTANT_SIZE = PrimaryKey.SIZE + 13; // type(1),
                                                                   // version(8),
                                                                   // keySize(4)

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    @Nullable
    private transient ByteBuffer bytes = null;
    private final Text key;
    private final PrimaryKey record;
    /**
     * Indicates the action that generated the Write. The type information is
     * recorded so that the Database knows how to apply the Write when accepting
     * it from a transport.
     */
    private final Action type;
    private final Value value;

    private final long version;

    /**
     * Construct a new instance.
     * 
     * @param type
     * @param key
     * @param value
     * @param record
     * @param version
     */
    private Write(Action type, Text key, Value value, PrimaryKey record,
            long version) {
        this(type, key, value, record, version, null);
    }

    /**
     * Construct a new instance.
     * 
     * @param type
     * @param key
     * @param value
     * @param record
     * @param version
     * @param bytes
     */
    private Write(Action type, Text key, Value value, PrimaryKey record,
            long version, @Nullable ByteBuffer bytes) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.record = record;
        this.version = version;
        this.bytes = bytes;
    }

    /**
     * {@inheritDoc}.
     * <p>
     * <strong>NOTE:</strong> The Write type is not taken into account when
     * determining hashCode or equality. To check for exact matches, including,
     * type, use {@link #matches(Write)}.
     * </p>
     */
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Write) {
            Write other = (Write) obj;
            return value.equals(other.value) && record.equals(other.record)
                    && key.equals(other.key);
        }
        return false;
    }

    /**
     * Return a byte buffer that represents this Write with the following order:
     * <ol>
     * <li><strong>keySize</strong> - position 0</li>
     * <li><strong>type</strong> - position 4</li>
     * <li><strong>version</strong> - position 5</li>
     * <li><strong>record</strong> - position 13</li>
     * <li><strong>key</strong> - position 21</li>
     * <li><strong>value</strong> - position(key) + keySize</li>
     * </ol>
     * 
     * @return the ByteBuffer representation
     */
    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = ByteBuffer.allocate(size());
            copyTo(bytes);
            bytes.rewind();
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    /**
     * Return the associated {@code key}.
     * 
     * @return the key
     */
    public Text getKey() {
        return key;
    }

    /**
     * Return the associated {@code record}.
     * 
     * @return the record
     */
    public PrimaryKey getRecord() {
        return record;
    }

    /**
     * Return the associated {@code type}.
     * 
     * @return the type
     */
    public Action getType() {
        return type;
    }

    /**
     * Return the associated {@code value}.
     * 
     * @return the value
     */
    public Value getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        return version;
    }

    /**
     * {@inheritDoc}.
     * <p>
     * <strong>NOTE:</strong> The Write type is not taken into account when
     * determining hashCode or equality. To check for exact matches, including,
     * type, use {@link #matches(Write)}.
     * </p>
     */
    @Override
    public int hashCode() {
        return Objects.hash(key, value, record);
    }

    @Override
    public boolean isStorable() {
        return version != NO_VERSION;
    }

    /**
     * Return a new {@link Write} (with a more recent and unique
     * {@link #version} that has the same {@link #key}, {@link #value} and
     * {@link #record} components as this write but has the inverse
     * {@link Write#type}.
     * 
     * @return a new {@link Write} that is the inverse of this one
     */
    public Write inverse() {
        if(type == Action.ADD) {
            return new Write(Action.REMOVE, key, value, record, Time.now());
        }
        else if(type == Action.REMOVE) {
            return new Write(Action.ADD, key, value, record, Time.now());
        }
        else {
            throw new UnsupportedOperationException(
                    "Cannot take the inversion of a comparison write");
        }
    }

    /**
     * Return {@code true} if this Write and {@code other} have the same
     * {@code type} and are equal.
     * 
     * @param other
     * @return {@code true} if this matches {@code other}.
     */
    public boolean matches(Write other) {
        return type == other.type && equals(other);
    }

    @Override
    public int size() {
        return CONSTANT_SIZE + key.size() + value.size();
    }

    @Override
    public String toString() {
        return type + " " + key + " AS " + value + " IN " + record + " AT "
                + version;
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        buffer.putInt(key.size());
        buffer.put((byte) type.ordinal());
        buffer.putLong(version);
        record.copyTo(buffer);
        key.copyTo(buffer);
        value.copyTo(buffer);
    }

}
