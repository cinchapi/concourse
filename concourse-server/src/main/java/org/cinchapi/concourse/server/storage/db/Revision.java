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
package org.cinchapi.concourse.server.storage.db;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.Versioned;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Preconditions;

/**
 * A Revision represents a modification involving a {@code locator}, {@code key}
 * and {@code value} at a {@code version} and is used to organize indexed data
 * that is permanently stored in a {@link Block} or viewed in a {@link Record}.
 * 
 * 
 * @author jnelson
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
    public static PrimaryRevision createPrimaryRevision(short uid, PrimaryKey record,
            Text key, Value value, long version, Action type) {
        return new PrimaryRevision(uid, record, key, value, version, type);
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
    public static SearchRevision createSearchRevision(short uid, Text key, Text word,
            Position position, long version, Action type) {
        return new SearchRevision(uid, key, word, position, version, type);
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
    public static SecondaryRevision createSecondaryRevision(short uid, Text key,
            Value value, PrimaryKey record, long version, Action type) {
        return new SecondaryRevision(uid, key, value, record, version, type);
    }

    /**
     * Indicates that a component of the class has variable length and therefore
     * must encode the size of that component for each instance.
     */
    static final int VARIABLE_SIZE = -1;

    /**
     * An field indicating the action performed to generate this Revision. This
     * information is recorded so that we can efficiently purge history while
     * maintaining consistent state.
     */
    private final Action type;
    
    /**
     * 
     */
    private final short uid;

    /**
     * The primary component used to locate the index, to which this Revision
     * belongs.
     */
    private final L locator;

    /**
     * The secondary component used to locate the field in the index, to which
     * this Revision belongs.
     */
    private final K key;

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
     * A cached copy of the binary representation that is returned from
     * {@link #getBytes()}.
     */
    private transient ByteBuffer bytes = null;

    /**
     * The number of bytes used to store the Revision. This value depends on
     * the number of variable sized components.
     */
    private transient final int size;

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
        this.uid = bytes.getShort();
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
    protected Revision(short uid, L locator, K key, V value, long version, Action type) {
        Preconditions.checkArgument(type != Action.COMPARE);
        this.type = type;
        this.uid = uid;
        this.locator = locator;
        this.key = key;
        this.value = value;
        this.version = version;
        this.size = 1 + 8 + 2 + (xLocatorSize() == VARIABLE_SIZE ? 4 : 0)
                + (xKeySize() == VARIABLE_SIZE ? 4 : 0) + locator.size()
                + key.size() + value.size();
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
            bytes.put((byte) type.ordinal());
            bytes.putShort(uid);
            bytes.putLong(version);
            if(xLocatorSize() == VARIABLE_SIZE) {
                bytes.putInt(locator.size());
            }
            bytes.put(locator.getBytes());
            if(xKeySize() == VARIABLE_SIZE) {
                bytes.putInt(key.size());
            }
            bytes.put(key.getBytes());
            bytes.put(value.getBytes());
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
     * 
     * @return
     */
    public short getUid() {
        return uid;
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
                + version + " BY " + uid;
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
