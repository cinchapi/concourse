/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.CommitVersions;
import com.cinchapi.concourse.server.storage.Versioned;
import com.google.common.annotations.VisibleForTesting;
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
public abstract class Revision<L extends Comparable<L> & Byteable, K extends Comparable<K> & Byteable, V extends Comparable<V> & Byteable>
        implements
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
    public static TableRevision createTableRevision(Identifier record, Text key,
            Value value, long version, Action type) {
        return new TableRevision(record, key, value, version, type);
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
    public static CorpusRevision createCorpusRevision(Text key, Text word,
            Position position, long version, Action type) {
        return new CorpusRevision(key, word, position, version, type);
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
    public static IndexRevision createIndexRevision(Text key, Value value,
            Identifier record, long version, Action type) {
        return new IndexRevision(key, value, record, version, type);
    }

    /**
     * Return {@code true} if {@code obj} is considered {@link Text#isCompact()}
     * (e.g. it is directly compact Text or it is a {@link Value} that wraps
     * compact Text}.
     * 
     * @param obj
     * @return a boolean that indicates if {@code obj} is compact Text.
     */
    @VisibleForTesting
    protected static boolean isCompactText(Object obj) {
        if(obj instanceof Text) {
            return ((Text) obj).isCompact();
        }
        else if(obj instanceof Value) {
            Object o2 = ((Value) obj).getObject();
            if(o2 instanceof Text) {
                return ((Text) o2).isCompact();
            }
        }
        return false;
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
     * Tracks when the {@link Revision} was
     * {@link #Revision(Comparable, Comparable, Comparable, long, Action)
     * created} or {@link #Revision(ByteBuffer) loaded}. Helps to disambiguate
     * and sequence {@link Revision Revisions} with the same {@link #version
     * version}.
     */
    private final transient long stamp;

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
     * The version that of this Revision. Revisions created within the same
     * commit may have the same version. Across commits, versions are
     * assumed to be a monotonically increasing value (e.g. timestamps).
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
        this.stamp = CommitVersions.next();
        this.bytes = bytes;
        this.size = bytes.remaining();
        this.type = Action.values()[bytes.get()];
        this.version = bytes.getLong();
        int limit = bytes.limit();

        // Locator
        int locatorSize = xLocatorSize() == VARIABLE_SIZE ? bytes.getInt()
                : xLocatorSize();
        bytes.limit(bytes.position() + locatorSize);
        this.locator = Byteables.readStatic(bytes, xLocatorClass());
        bytes.limit(limit);

        // Key
        int keySize = xKeySize() == VARIABLE_SIZE ? bytes.getInt() : xKeySize();
        bytes.limit(bytes.position() + keySize);
        this.key = Byteables.readStatic(bytes, xKeyClass());
        bytes.limit(limit);

        // Locator
        this.value = Byteables.readStatic(bytes, xValueClass());
        bytes.limit(limit);
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
        this.stamp = CommitVersions.next();
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
     * Copy a byte sequence that represents this Revision with the following
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
     */
    @Override
    public void copyTo(ByteSink sink) {
        sink.put((byte) type.ordinal());
        sink.putLong(version);
        if(xLocatorSize() == VARIABLE_SIZE) {
            sink.putInt(locator.size());
        }
        locator.copyTo(sink);
        if(xKeySize() == VARIABLE_SIZE) {
            sink.putInt(key.size());
        }
        key.copyTo(sink);
        value.copyTo(sink);
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

    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            bytes = Byteable.super.getBytes();
        }
        ByteBuffer bytes = ByteBuffers.asReadOnlyBuffer(this.bytes);
        if(isCompactText(locator) || isCompactText(key)
                || isCompactText(value)) {
            // If any component of this Revision is compact Text, don't cache
            // the #bytes in memory so that the desire for space efficiency is
            // maintained.
            this.bytes = null;
        }
        return bytes;
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
    public long stamp() {
        return stamp;
    }

    @Override
    public String toString() {
        return type + " " + key + " AS " + value + " IN " + locator + " AT "
                + version;
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
