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

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.Composite;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Syncable;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * A reference that stores the start and end position for sequences of bytes
 * that relate to a key that is described by one or more {@link Byteable}
 * objects. A BlockIndex is associated with each {@link Block} to determine
 * where to look on disk for a particular {@code locator} or {@code locator}/
 * {@code key} pair.
 * 
 * @author jnelson
 */
public class BlockIndex implements Byteable, Syncable {

    /**
     * Return a newly created BlockIndex.
     * 
     * @param file
     * @param expectedInsertions
     * @return the BlockIndex
     */
    public static BlockIndex create(String file, int expectedInsertions) {
        return new BlockIndex(file, expectedInsertions);
    }

    /**
     * Return the BlockIndex that is stored in {@code file}.
     * 
     * @param file
     * @return the BlockIndex
     */
    public static BlockIndex open(String file) {
        return new BlockIndex(file);
    }

    /**
     * Represents an entry that has not been recorded.
     */
    public static final int NO_ENTRY = -1;

    /**
     * The entries contained in the index.
     */
    private Map<Composite, Entry> entries;

    /**
     * The file where the BlockIndex is stored during an diskSync.
     */
    private final String file;

    /**
     * Lock used to ensure the object is ThreadSafe. This lock provides access
     * to a masterLock.readLock()() and masterLock.writeLock()().
     */
    private final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    /**
     * A flag that indicates if this index is mutable. An index is no longer
     * mutable after it has been synced.
     */
    private boolean mutable;

    /**
     * The running size of the index in bytes.
     */
    private transient int size = 0;

    /**
     * A {@link SoftReference} to the entries contained in the index that is
     * used to reduce memory overhead.
     */
    private SoftReference<Map<Composite, Entry>> softEntries;

    /**
     * Lazily construct an existing instance from the data in {@code file}.
     * 
     * @param file
     */
    public BlockIndex(String file) {
        this.file = file;
        this.mutable = false;
        this.entries = null;
        this.softEntries = null;
    }

    /**
     * Construct a new instance.
     * 
     * @param expectedInsertions
     */
    private BlockIndex(String file, int expectedInsertions) {
        this.file = file;
        this.entries = Maps.newHashMapWithExpectedSize(expectedInsertions);
        this.softEntries = null;
        this.mutable = true;
    }

    @Override
    public ByteBuffer getBytes() {
        Preconditions.checkState(mutable);
        masterLock.readLock().lock();
        try {
            ByteBuffer bytes = ByteBuffer.allocate(size());
            copyToByteBuffer(bytes);
            bytes.flip();
            return bytes;
        }
        finally {
            masterLock.readLock().unlock();
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
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(byteables);
            Entry entry = entries().get(composite);
            if(entry != null) {
                return entry.getEnd();
            }
            else {
                return NO_ENTRY;
            }
        }
        finally {
            masterLock.readLock().unlock();
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
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(byteables);
            Entry entry = entries().get(composite);
            if(entry != null) {
                return entry.getStart();
            }
            else {
                return NO_ENTRY;
            }
        }
        finally {
            masterLock.readLock().unlock();
        }
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
        Preconditions.checkState(mutable);
        masterLock.writeLock().lock();
        try {
            Composite composite = Composite.create(byteables);
            Entry entry = entries().get(composite);
            Preconditions.checkState(entry != null,
                    "Cannot set the end position before setting "
                            + "the start position. Tried to put %s", end);
            entry.setEnd(end);
        }
        finally {
            masterLock.writeLock().unlock();
        }
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
        Preconditions.checkState(mutable);
        masterLock.writeLock().lock();
        try {
            Composite composite = Composite.create(byteables);
            Entry entry = entries().get(composite);
            if(entry == null) {
                entry = new Entry(composite);
                entries.put(composite, entry);
                size += entry.size() + 4;
            }
            entry.setStart(start);
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        masterLock.readLock().lock();
        try {
            return size;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public void sync() {
        Preconditions.checkState(mutable);
        masterLock.readLock().lock();
        FileChannel channel = FileSystem.getFileChannel(file);
        try {
            channel.write(getBytes());
            softEntries = new SoftReference<Map<Composite, Entry>>(entries);
            mutable = false;
            entries = null;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileSystem.closeFileChannel(channel); // CON-162
            masterLock.readLock().unlock();
        }
    }

    @Override
    public void copyToByteBuffer(ByteBuffer buffer) {
        Preconditions.checkState(mutable);
        masterLock.readLock().lock();
        try {
            for (Entry entry : entries.values()) {
                buffer.putInt(entry.size());
                entry.copyToByteBuffer(buffer);
            }
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return {@code true} if this index is considered <em>loaded</em> meaning
     * all of its entries are available in memory.
     * 
     * @return {@code true} if the entries are loaded
     */
    protected boolean isLoaded() { // visible for testing
        masterLock.readLock().lock();
        try {
            return mutable
                    || (softEntries != null && softEntries.get() != null);
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the entries in this index. This method will lazily load the
     * entries on demand if they do not currently exist in memory.
     * 
     * @return the entries
     */
    private synchronized Map<Composite, Entry> entries() {
        if(mutable && entries != null) {
            return entries;
        }
        else if(!mutable && (softEntries == null || softEntries.get() == null)) { // do
                                                                                  // lazy
                                                                                  // load
            ByteBuffer bytes = FileSystem.map(file, MapMode.READ_ONLY, 0,
                    FileSystem.getFileSize(file));
            Iterator<ByteBuffer> it = ByteableCollections.iterator(bytes);
            Map<Composite, Entry> entries = Maps
                    .newHashMapWithExpectedSize(bytes.capacity()
                            / Entry.CONSTANT_SIZE);
            while (it.hasNext()) {
                Entry entry = new Entry(it.next());
                entries.put(entry.getKey(), entry);
            }
            softEntries = new SoftReference<Map<Composite, Entry>>(entries);
            return softEntries.get();
        }
        else if(!mutable && softEntries.get() != null) {
            return softEntries.get();
        }
        else {
            // "If i'm really an engineer thats worth a damn, we won't ever get
            // to this point" -jnelson
            throw new IllegalStateException();
        }
    }

    /**
     * Represents a single entry in the Index.
     * 
     * @author jnelson
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
            this.key = Composite.fromByteBuffer(ByteBuffers.get(bytes,
                    bytes.remaining()));
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
        public ByteBuffer getBytes() {
            ByteBuffer bytes = ByteBuffer.allocate(size());
            copyToByteBuffer(bytes);
            bytes.flip();
            return bytes;
        }

        /**
         * Return the end position.
         * 
         * @return the end
         */
        public int getEnd() {
            return end;
        }

        /**
         * Return the entry key
         * 
         * @return the key
         */
        public Composite getKey() {
            return key;
        }

        /**
         * Return the start position.
         * 
         * @return the start
         */
        public int getStart() {
            return start;
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

        @Override
        public void copyToByteBuffer(ByteBuffer buffer) {
            buffer.putInt(start);
            buffer.putInt(end);
            key.copyToByteBuffer(buffer);
        }

    }

}