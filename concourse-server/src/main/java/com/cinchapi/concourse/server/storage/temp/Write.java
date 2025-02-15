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
package com.cinchapi.concourse.server.storage.temp;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.CommitVersions;
import com.cinchapi.concourse.server.storage.Versioned;
import com.cinchapi.concourse.server.storage.cache.ByteableFunnel;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * A {@link Write} is a temporary representation of data before it is
 * {@link com.cinchapi.concourse.server.storage.DurableStore durably} stored and
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
                Identifier.of(record), CommitVersions.next());
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
        Identifier record = Identifier
                .fromByteBuffer(ByteBuffers.get(bytes, Identifier.SIZE));
        Text key = Text.fromByteBuffer(ByteBuffers.get(bytes, keySize));
        Value value = Value
                .fromByteBuffer(ByteBuffers.get(bytes, bytes.remaining()));
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
                Value.wrap(value), Identifier.of(record), NO_VERSION);
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
        return new Write(Action.REMOVE, Text.wrapCached(key), Value.wrap(value),
                Identifier.of(record), CommitVersions.next());
    }

    /**
     * The minimum number of bytes needed to encode every Write.
     */
    private static final int CONSTANT_SIZE = Identifier.SIZE + 13; // type(1),
                                                                   // version(8),
                                                                   // keySize(4)

    /**
     * The minimum number of bytes needed to encode every Write.
     */
    // @formatter:off
    public static final int MINIMUM_SIZE = 
            CONSTANT_SIZE
            + 1 // minimum key size since it cannot be empty
            + 1 // value type
            + 1 // minimum value size since it cannot be empty
    ; 
    // @formatter:on

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    @Nullable
    private transient ByteBuffer bytes = null;

    /**
     * The {@link #getKey() key}.
     */
    private final Text key;

    /**
     * The {@link #getRecord() record}.
     */
    private final Identifier record;

    /**
     * Tracks when this {@link Write} was created or
     * {@link #fromByteBuffer(ByteBuffer) loaded}.
     */
    private final transient long stamp;
    /**
     * Indicates the action that generated the Write. The type information is
     * recorded so that the Database knows how to apply the Write when accepting
     * it from a transport.
     */
    private final Action type;

    /**
     * The {@link #getValue() value}.
     */
    private final Value value;

    /**
     * The {@link #getVersion() version}.
     */
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
    private Write(Action type, Text key, Value value, Identifier record,
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
    private Write(Action type, Text key, Value value, Identifier record,
            long version, @Nullable ByteBuffer bytes) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.record = record;
        this.version = version;
        this.bytes = bytes;
        this.stamp = CommitVersions.next();
    }

    /**
     * Copy a byte sequence that represents this Write with the following order:
     * <ol>
     * <li><strong>keySize</strong> - position 0</li>
     * <li><strong>type</strong> - position 4</li>
     * <li><strong>version</strong> - position 5</li>
     * <li><strong>record</strong> - position 13</li>
     * <li><strong>key</strong> - position 21</li>
     * <li><strong>value</strong> - position(key) + keySize</li>
     * </ol>
     */
    @Override
    public void copyTo(ByteSink sink) {
        if(bytes != null) {
            sink.put(getBytes());
        }
        else {
            sink.putInt(key.size());
            sink.put((byte) type.ordinal());
            sink.putLong(version);
            record.copyTo(sink);
            key.copyTo(sink);
            value.copyTo(sink);
        }
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

    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = Byteable.super.getBytes();
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
    public Identifier getRecord() {
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
     * Return a {@link HashCode} for this {@link Write Write's} topic (e.g.
     * {@link #getKey() key}, {@link #getValue() value} and {@link #getRecord()
     * record}) and commit {@link #getVersion() version}.
     * <p>
     * The hash does not consider the {@link #getType() type}. It is assumed
     * that a single commit version will only contain one instance of a
     * {@link Write} topic, so the {@link #getType() type} isn't necessary when
     * using the hash to detect if a duplicate {@link Write} exists during
     * {@link com.cinchapi.concourse.server.storage.DurableStore#reconcile(java.util.Set)
     * reconciliation}
     * </p>
     * 
     * @return the {@link HashCode}
     */
    public HashCode hash() {
        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putLong(version);
        hasher.putObject(Composite.create(key, value, record),
                ByteableFunnel.INSTANCE);
        return hasher.hash();
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
            return new Write(Action.REMOVE, key, value, record,
                    CommitVersions.next());
        }
        else if(type == Action.REMOVE) {
            return new Write(Action.ADD, key, value, record,
                    CommitVersions.next());
        }
        else {
            throw new UnsupportedOperationException(
                    "Cannot take the inversion of a comparison write");
        }
    }

    @Override
    public boolean isStorable() {
        return version != NO_VERSION;
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

    /**
     * Return a new {@link Write} that contains the same elements, at the
     * specified {@code version}.
     * 
     * @return a redone {@link Write}
     */
    public Write rewrite(long version) {
        return new Write(type, key, value, record, version);
    }

    @Override
    public int size() {
        return CONSTANT_SIZE + key.size() + value.size();
    }

    @Override
    public long stamp() {
        return stamp;
    }

    @Override
    public String toString() {
        return type + " " + key + " AS " + value + " IN " + record + " AT "
                + version;
    }

}
