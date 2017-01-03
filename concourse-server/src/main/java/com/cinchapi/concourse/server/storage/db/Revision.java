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
package com.cinchapi.concourse.server.storage.db;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.Versioned;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.base.Preconditions;

/**
 * A Revision represents a modification involving a {@code locator}, {@code key}
 * and {@code value} at a {@code version} and is used to organize indexed data
 * that is permanently stored in a {@link Block} or viewed in a {@link Record}.
 * 
 * 
 * @author Jeff Nelson
 * @param <L> - the locator type
 * @param <K> - the key type
 * @param <V> - the value type
 */
@Immutable
public abstract class Revision<L extends Comparable<L> & Byteable, K extends Comparable<K> & Byteable, V extends Comparable<V> & Byteable> implements
        Byteable,
        Versioned {

    /**
     * Create a PrimaryRevision for {@code key} as {@code value} in
     * {@code record} at {@code version}.
     * 
     * @param record
     * @param key
     * @param value
     * @param version
     * @param type
     * 
     * @return the PrimaryRevision
     */
    public static PrimaryRevision createPrimaryRevision(PrimaryKey record,
            Text key, Value value, long version, Action type) {
        return new PrimaryRevision(record, key, value, version, type);
    }

    /**
     * Create a SearchRevision for {@code word} at {@code position} for
     * {@code key} at {@code version}.
     * 
     * @param key
     * @param word
     * @param position
     * @param version
     * @param type
     * @return the SearchRevision
     */
    public static SearchRevision createSearchRevision(Text key, Text word,
            Position position, long version, Action type) {
        return new SearchRevision(key, word, position, version, type);
    }

    /**
     * Create a SecondaryRevision for {@code key} as {@code value} in
     * {@code record} at {@code version}.
     * 
     * @param key
     * @param value
     * @param record
     * @param version
     * @param type
     * @return the SecondaryRevision
     */
    public static SecondaryRevision createSecondaryRevision(Text key,
            Value value, PrimaryKey record, long version, Action type) {
        return new SecondaryRevision(key, value, record, version, type);
    }

    /**
     * Indicates that a component of the class has variable length and therefore
     * must encode the size of that component for each instance.
     */
    static final int VARIABLE_SIZE = -1;

    /**
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    private transient ByteBuffer bytes = null;

    /**
     * The secondary component used to locate the field in the index, to which
     * this Revision belongs.
     */
    private final K key;

    /**
     * The primary component used to locate the index, to which this Revision
     * belongs.
     */
    private final L locator;

    /**
     * The number of bytes used to store the Revision. This value depends on
     * the number of variable sized components.
     */
    private transient final int size;

    /**
     * An field indicating the action performed to generate this Revision. This
     * information is recorded so that we can efficiently purge history while
     * maintaining consistent state.
     */
    private final Action type;

    /**
     * The tertiary component that typically represents the payload for what
     * this Revision represents.
     */
    private final V value;

    /**
     * The unique version that identifies this Revision. Versions are assumed to
     * be an atomically increasing values (i.e. timestamps).
     */
    private final long version;

    /**
     * Construct an instance that represents an existing Revision from a
     * ByteBuffer. This constructor is public so as to comply with the
     * {@link Byteable} interface. Calling this constructor directly is not
     * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
     * advantage of reference caching.
     * 
     * @param bytes
     */
    /*
     * (non-Javadoc)
     * This constructor exists and is public so that subclass instances can be
     * dynamically deserialized using the Byteables#read() method.
     */
    @DoNotInvoke
    public Revision(ByteBuffer bytes) {
        this.bytes = bytes;
        this.type = Action.values()[bytes.get()];
        this.version = bytes.getLong();
        this.locator = Byteables.readStatic(ByteBuffers.get(bytes,
                xLocatorSize() == VARIABLE_SIZE ? bytes.getInt()
                        : xLocatorSize()), xLocatorClass());
        this.key = Byteables.readStatic(ByteBuffers.get(bytes,
                xKeySize() == VARIABLE_SIZE ? bytes.getInt() : xKeySize()),
                xKeyClass());
        this.value = Byteables.readStatic(
                ByteBuffers.get(bytes, bytes.remaining()), xValueClass());
        this.size = bytes.capacity();
    }

    /**
     * Construct a new instance.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     */
    protected Revision(L locator, K key, V value, long version, Action type) {
        Preconditions.checkArgument(type != Action.COMPARE);
        this.type = type;
        this.locator = locator;
        this.key = key;
        this.value = value;
        this.version = version;
        this.size = 1 + 8 + (xLocatorSize() == VARIABLE_SIZE ? 4 : 0)
                + (xKeySize() == VARIABLE_SIZE ? 4 : 0) + locator.size()
                + key.size() + value.size();
    }

    /**
     * Return a {@link CompactRevision} that is appropriate to store in a
     * {@link Record records} history collection. After calling this method, it
     * is okay to set this instance equal to {@code null}.
     * 
     * @return the compact form of this Revision.
     */
    public CompactRevision<V> compact() {
        return new CompactRevision<V>(value, version, type);
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE: The Revision type is NOT considered when determining
     * equality.</strong>
     * </p>
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if(obj.getClass() == this.getClass()) {
            Revision<L, K, V> other = (Revision<L, K, V>) obj;
            return locator.equals(other.locator) && key.equals(other.key)
                    && value.equals(other.value);
        }
        return false;
    }

    /**
     * Return a byte buffer that represents this Revision with the following
     * order:
     * <ol>
     * <li><strong>version</strong></li>
     * <li><strong>locatorSize</strong> -
     * <em>if {@link #xLocatorSize()} == {@link #VARIABLE_SIZE}</em></li>
     * <li><strong>locator</strong></li>
     * <li><strong>keySize</strong> -
     * <em>if {@link #xKeySize()} == {@link #VARIABLE_SIZE}</em></li>
     * <li><strong>key</strong></li>
     * <li><strong>value</strong></li>
     * 
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
     * Return the {@link #key} associated with this Revision.
     * 
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Return the {@link #locator} associated with this Revision.
     * 
     * @return the locator
     */
    public L getLocator() {
        return locator;
    }

    /**
     * Return the {@link #type} associated with this Revision.
     * 
     * @return the type
     */
    public Action getType() {
        return type;
    }

    /**
     * Return the {@link #value} associated with this Revision.
     * 
     * @return the value
     */
    public V getValue() {
        return value;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(locator, key, value);
    }

    @Override
    public boolean isStorable() {
        return version != NO_VERSION;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String toString() {
        return type + " " + key + " AS " + value + " IN " + locator + " AT "
                + version;
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        buffer.put((byte) type.ordinal());
        buffer.putLong(version);
        if(xLocatorSize() == VARIABLE_SIZE) {
            buffer.putInt(locator.size());
        }
        locator.copyTo(buffer);
        if(xKeySize() == VARIABLE_SIZE) {
            buffer.putInt(key.size());
        }
        key.copyTo(buffer);
        value.copyTo(buffer);
    }

    /**
     * Return the class of the {@link #key} type.
     * 
     * @return they key class
     */
    protected abstract Class<K> xKeyClass();

    /**
     * Return the size used to store each {@link #key}. If this value is not
     * fixed, return {@link #VARIABLE_SIZE}.
     * 
     * @return the key size
     */
    protected abstract int xKeySize();

    /**
     * Return the class of the {@link locator} type.
     * 
     * @return the locator class
     */
    protected abstract Class<L> xLocatorClass();

    /**
     * Return the size used to store each {@link #locator}. If this value is not
     * fixed, return {@link #VARIABLE_SIZE}.
     * 
     * @return the locator size
     */
    protected abstract int xLocatorSize();

    /**
     * Return the class of the {@link #value} type.
     * 
     * @return the value class
     */
    protected abstract Class<V> xValueClass();

}
