/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A {@link Manifest} stores and provides the efficient lookup for the start and
 * end position for sequences of bytes that relate to a key that is described by
 * one or more {@link Byteable} objects.
 * <p>
 * A {@link Manifest} is associated with each {@link Chunk} to determine where
 * to look on disk for a particular {@code locator} or
 * {@code locator}/{@code key} pair.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class Manifest extends TransferableByteSequence {

    /**
     * Create a new {@link Manifest} that is expected to have
     * {@code expectedInsertions} entries.
     * 
     * @param expectedInsertions
     * @return the new {@link Manifest}
     */
    public static Manifest create(int expectedInsertions) {
        return new Manifest(expectedInsertions);
    }

    /**
     * Load an existing {@link Manifest} from {@code file}.
     * 
     * @param file
     * @param position
     * @param length
     * @return the loaded {@link Manifest}
     */
    public static Manifest load(Path file, long position, long length) {
        return new Manifest(file, position, length);
    }

    /**
     * Represents an entry that has not been recorded.
     */
    public static final int NO_ENTRY = -1;

    /**
     * A {@link SoftReference} to the entries contained in the {@link Manifest}
     * that is used to reduce memory overhead.
     * 
     * <p>
     * It is {@code null} until the
     * {@link Manifest} is {@link #freeze(Path, long) frozen} or the
     * {@link #entries()} are loaded
     * </p>
     */
    @Nullable
    private SoftReference<Map<Composite, Entry>> $entries;

    /**
     * The entries contained in the {@link Manifest}.
     * <p>
     * It is {@code null} if the
     * {@link Manifest} has been {@link #freeze(Path, long) frozen}.
     * </p>
     */
    @Nullable
    private Map<Composite, Entry> entries;

    /**
     * The running size of the {@link Manifest} in bytes.
     */
    private long length = 0;

    /**
     * Construct a new instance.
     * 
     * @param expectedInsertions
     */
    private Manifest(int expectedInsertions) {
        super();
        this.length = 0;
        this.entries = Maps
                .newLinkedHashMapWithExpectedSize(expectedInsertions);
        this.$entries = null;
    }

    /**
     * Load an existing instance.
     * 
     * @param file
     * @param position
     * @param length
     */
    private Manifest(Path file, long position, long length) {
        super(file, position, length);
        this.length = length;
        this.entries = null;
        this.$entries = null;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Manifest) {
            Manifest other = (Manifest) obj;
            return entries().equals(other.entries());
        }
        else {
            return false;
        }
    }

    /**
     * Return the end position for {@code byteables} if it exists, otherwise
     * return {@link #NO_ENTRY}.
     * 
     * @param byteables
     * @return the end position
     */
    public int getEnd(Byteable... byteables) {
        Composite composite = Composite.create(byteables);
        Entry entry = entries().get(composite);
        if(entry != null) {
            return entry.end();
        }
        else {
            return NO_ENTRY;
        }
    }

    /**
     * Return the start position for {@code byteables} if it exists, otherwise
     * return {@code #NO_ENTRY}.
     * 
     * @param byteables
     * @return the start position
     */
    public int getStart(Byteable... byteables) {
        Composite composite = Composite.create(byteables);
        Entry entry = entries().get(composite);
        if(entry != null) {
            return entry.start();
        }
        else {
            return NO_ENTRY;
        }
    }

    @Override
    public int hashCode() {
        return entries().hashCode();
    }

    @Override
    public long length() {
        return length;
    }

    /**
     * Record the end position for the {@code byteables}.
     * 
     * @param end
     * @param byteables
     */
    public void putEnd(int end, Byteable... byteables) {
        Preconditions.checkArgument(end >= 0,
                "Cannot have negative index. Tried to put %s", end);
        Preconditions.checkState(isMutable());
        Composite composite = Composite.create(byteables);
        Entry entry = entries().get(composite);
        Preconditions.checkState(entry != null,
                "Cannot set the end position before setting "
                        + "the start position. Tried to put %s",
                end);
        entry.setEnd(end);
    }

    /**
     * Record the start position for the {@code byteables}.
     * 
     * @param start
     * @param byteables
     */
    public void putStart(int start, Byteable... byteables) {
        Preconditions.checkArgument(start >= 0,
                "Cannot have negative index. Tried to put %s", start);
        Preconditions.checkState(isMutable());
        Composite composite = Composite.create(byteables);
        Entry entry = entries().get(composite);
        if(entry == null) {
            entry = new Entry(composite);
            entries.put(composite, entry);
            length += entry.size() + 4;
        }
        entry.setStart(start);
    }

    @Override
    protected void flush(ByteSink sink) {
        for (Entry entry : entries().values()) {
            sink.putInt(entry.size());
            entry.copyTo(sink);
        }
    }

    @Override
    protected void free() {
        this.$entries = new SoftReference<Map<Composite, Entry>>(entries);
        this.entries = null; // Make eligible for GC
    }

    /**
     * Return {@code true} if this {@link Manifest} is considered
     * <em>loaded</em> meaning all of its entries are available in memory.
     * 
     * @return {@code true} if the entries are loaded
     */
    protected boolean isLoaded() { // visible for testing
        return isMutable() || ($entries != null && $entries.get() != null);
    }

    /**
     * Return the entries in this index. This method will lazily load the
     * entries on demand if they do not currently exist in memory.
     * 
     * @return the entries
     */
    private synchronized Map<Composite, Entry> entries() {
        if(entries != null) {
            return entries;
        }
        else {
            while ($entries == null || $entries.get() == null) {
                ByteBuffer bytes = FileSystem.map(file(), MapMode.READ_ONLY,
                        position(), length);
                Iterator<ByteBuffer> it = ByteableCollections.iterator(bytes);
                Map<Composite, Entry> entries = Maps
                        .newLinkedHashMapWithExpectedSize(
                                bytes.capacity() / Entry.CONSTANT_SIZE);
                while (it.hasNext()) {
                    Entry entry = new Entry(it.next());
                    entries.put(entry.key(), entry);
                }
                $entries = new SoftReference<Map<Composite, Entry>>(entries);
            }
            return $entries.get();
        }
    }

    /**
     * Represents a single entry in the {@link Manifest}.
     * 
     * @author Jeff Nelson
     */
    private final class Entry implements Byteable {

        private static final int CONSTANT_SIZE = 8; // start(4), end(4)

        private int end = NO_ENTRY;
        private final Composite key;
        private int start = NO_ENTRY;

        /**
         * Construct an instance that represents an existing Entry from a
         * ByteBuffer. This constructor is public so as to comply with the
         * {@link Byteable} interface. Calling this constructor directly is not
         * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
         * advantage of reference caching.
         * 
         * @param bytes
         */
        public Entry(ByteBuffer bytes) {
            this.start = bytes.getInt();
            this.end = bytes.getInt();
            this.key = Composite
                    .load(ByteBuffers.get(bytes, bytes.remaining()));
        }

        /**
         * Construct a new instance.
         * 
         * @param key
         */
        public Entry(Composite key) {
            this.key = key;
        }

        @Override
        public void copyTo(ByteSink sink) {
            sink.putInt(start);
            sink.putInt(end);
            key.copyTo(sink);
        }

        /**
         * Return the end position.
         * 
         * @return the end
         */
        public int end() {
            return end;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Entry) {
                Entry other = (Entry) obj;
                return start == other.start && end == other.end
                        && key.equals(other.key);
            }
            else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, start, end);
        }

        /**
         * Return the entry key
         * 
         * @return the key
         */
        public Composite key() {
            return key;
        }

        /**
         * Set the end position.
         * 
         * @param end the end to set
         */
        public void setEnd(int end) {
            this.end = end;
        }

        /**
         * Set the start position.
         * 
         * @param start the start to set
         */
        public void setStart(int start) {
            this.start = start;
        }

        @Override
        public int size() {
            return CONSTANT_SIZE + key.size();
        }

        /**
         * Return the start position.
         * 
         * @return the start
         */
        public int start() {
            return start;
        }

    }
}
