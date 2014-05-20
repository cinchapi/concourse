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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.Byteables;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.io.Syncable;
import org.cinchapi.concourse.server.storage.Action;
import org.cinchapi.concourse.server.storage.cache.BloomFilter;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.TreeMultiset;
import com.google.common.primitives.Longs;

/**
 * <p>
 * A Block is a sorted collection of Revisions that is used by the Database to
 * store indexed data. When a Block is initially created, it resides solely in
 * memory and is able to insert new revisions, which are sorted on the fly by a
 * {@link Sorter}. Once the Block is synced to disk it becomes immutable and all
 * lookups are disk based. This means that writing to a block never incurs any
 * random disk I/O. A Block is not durable until the {@link #sync()} method is
 * called, so Block serialization and Buffer.Page deletion happen sequentially.
 * </p>
 * <p>
 * Each Block is stored with a corresponding {@link BloomFilter} and a
 * {@link BlockIndex} to make lookups more efficient. The BlockFilter is used to
 * test whether a Revision involving some locator and possibly key, and possibly
 * value <em>might</em> exist in the Block. The BlockIndex is used to find the
 * exact start and end positions for Revisions involving a locator and possibly
 * some key. This means that reading from a Block never incurs any unnecessary
 * disk I/O.
 * </p>
 * <p>
 * Prior to 0.2, Concourse stored each logical Record in its own file, which had
 * the advantage of simplified deserialization (we only needed to locate one
 * file and read all of its content). The down side to that approach was that a
 * single record couldn't be deserialized if it was larger than the amount of
 * available memory. Storing data in blocks, helps to solve that problem,
 * because larger records are broken up and we can do more granular seeking to
 * reduce the amount of data that must come into memory (i.e. we can limit data
 * reading by timestamp). Blocks also make it much easier to nuke old data
 * without reading anything. And since each Block has its own bloom filter in
 * memory, we make best efforts to only look at files when necessary.
 * </p>
 * 
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
abstract class Block<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> implements
        Byteable,
        Syncable {

    /**
     * Return a new PrimaryBlock that will be stored in {@code directory}.
     * 
     * @param id
     * @param directory
     * @return the PrimaryBlock
     */
    public static PrimaryBlock createPrimaryBlock(String id, String directory) {
        return new PrimaryBlock(id, directory, false);
    }

    /**
     * Return a new SearchBlock that will be stored in {@code directory}.
     * 
     * @param id
     * @param directory
     * @return the SearchBlock
     */
    public static SearchBlock createSearchBlock(String id, String directory) {
        return new SearchBlock(id, directory, false);
    }

    /**
     * Return a new SecondaryBlock that will be stored in {@code directory}.
     * 
     * @param id
     * @param directory
     * @return the SecondaryBlock
     */
    public static SecondaryBlock createSecondaryBlock(String id,
            String directory) {
        return new SecondaryBlock(id, directory, false);
    }

    /**
     * Return the block id from the name of the block file.
     * 
     * @param filename
     * @return the block id
     */
    public static String getId(String filename) {
        return FileSystem.getSimpleName(filename);
    }

    /**
     * The expected number of Block insertions. This number is used to size the
     * Block's internal data structures. This value should be large enough to
     * reflect the fact that, for each revision, we make 3 inserts into the
     * bloom filter, but no larger than necessary since we must keep all bloom
     * filters in memory.
     */
    private static final int EXPECTED_INSERTIONS = GlobalState.BUFFER_PAGE_SIZE;

    /**
     * The extension for the block file.
     */
    @PackagePrivate
    static final String BLOCK_NAME_EXTENSION = ".blk";

    /**
     * The extension for the {@link BloomFilter} file.
     */
    private static final String FILTER_NAME_EXTENSION = ".fltr";

    /**
     * The extension for the {@link BlockIndex} file.
     */
    private static final String INDEX_NAME_EXTENSION = ".indx";

    /**
     * The master lock for {@link #write} and {@link #read}. DO NOT use this
     * lock directly.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * An exclusive lock that permits only one writer and no reader. Use this
     * lock to ensure that no seek occurs while data is being inserted into the
     * Block.
     */
    protected final WriteLock write = master.writeLock();

    /**
     * A shared lock that permits many readers and no writer. Use this lock to
     * ensure that no data insert occurs while a seek is happening within the
     * Block.
     */
    protected final ReadLock read = master.readLock();

    /**
     * The flag that indicates whether the Block is mutable or not. A Block is
     * mutable until a call to {@link #sync()} stores it to disk.
     */
    protected transient boolean mutable;

    /**
     * The running size of the Block. This number only refers to the size of the
     * Revisions that are stored in the block file. The size for the filter and
     * index are tracked separately.
     */
    private transient int size;

    /**
     * The location of the block file.
     */
    private final String file;

    /**
     * The unique id for the block. Each component of the block is named after
     * the id. It is assumed that block ids should be assigned in atomically
     * increasing order (i.e. a timestamp).
     */
    private final String id;

    /**
     * A collection that contains all the Revisions that have been inserted into
     * the Block. This collection is sorted on the fly as elements are inserted.
     * This collection is only maintained for a mutable Block. A Block that is
     * synced and subsequently read from disk does not rely on this collection
     * at all.
     */
    @Nullable
    private TreeMultiset<Revision<L, K, V>> revisions;

    /**
     * A fixed size filter that is used to test whether elements are contained
     * in the Block without actually looking through the Block.
     */
    private final BloomFilter filter;

    /**
     * The index to determine which bytes in the block pertain to a locator or
     * locator/key pair.
     */
    private final BlockIndex index; // Since the index is only used for
                                    // immutable blocks, it is only populated
                                    // during the call to #getBytes()

    /**
     * Construct a new instance.
     * 
     * @param id
     * @param directory
     * @param diskLoad - set to {@code true} to deserialize the block {@code id}
     *            from {@code directory} on disk
     */
    protected Block(String id, String directory, boolean diskLoad) {
        FileSystem.mkdirs(directory);
        this.id = id;
        this.file = directory + File.separator + id + BLOCK_NAME_EXTENSION;
        if(diskLoad) {
            this.mutable = false;
            this.size = (int) FileSystem.getFileSize(this.file);
            this.filter = BloomFilter.open(directory + File.separator + id
                    + FILTER_NAME_EXTENSION);
            this.index = BlockIndex.open(directory + File.separator + id
                    + INDEX_NAME_EXTENSION);
            this.revisions = null;
        }
        else {
            this.mutable = true;
            this.size = 0;
            this.revisions = TreeMultiset.create(Sorter.INSTANCE);
            this.filter = BloomFilter.create(
                    (directory + File.separator + id + FILTER_NAME_EXTENSION),
                    EXPECTED_INSERTIONS);
            this.index = BlockIndex.create(directory + File.separator + id
                    + INDEX_NAME_EXTENSION, EXPECTED_INSERTIONS);
        }
    }

    @Override
    public ByteBuffer getBytes() {
        read.lock();
        try {
            ByteBuffer bytes = ByteBuffer.allocate(size);
            L locator = null;
            K key = null;
            int position = 0;
            for (Revision<L, K, V> revision : revisions) {
                bytes.putInt(revision.size());
                bytes.put(revision.getBytes());
                position = bytes.position() - revision.size() - 4;
                /*
                 * States that trigger this condition to be true:
                 * 1. This is the first locator we've seen
                 * 2. This locator is different than the last one we've seen
                 */
                if(locator == null || !locator.equals(revision.getLocator())) {
                    index.putStart(position, revision.getLocator());
                    if(locator != null) {
                        // There was a locator before us (we are not the first!)
                        // and we need to record the end index.
                        index.putEnd(position - 1, locator);
                    }
                }
                /*
                 * NOTE: IF key == null, then it must be the case that locator
                 * == null since they are set at the same time. Therefore we do
                 * not need to explicitly check for that condition below
                 * 
                 * States that trigger this condition to be true:
                 * 1. This is the first key we've seen
                 * 2. This key is different than the last one we've seen
                 * (regardless of whether the locator is different or the same!)
                 * 3. This key is the same as the last one we've seen, but the
                 * locator is different.
                 */
                if(key == null || !key.equals(revision.getKey())
                        || !locator.equals(revision.getLocator())) {
                    index.putStart(position, revision.getLocator(),
                            revision.getKey());
                    if(key != null) {
                        // There was a locator, key before us (we are not the
                        // first!) and we need to record the end index.
                        index.putEnd(position - 1, locator, key);
                    }
                }
                locator = revision.getLocator();
                key = revision.getKey();
            }
            if(revisions.size() > 0) {
                position = bytes.position() - 1;
                index.putEnd(position, locator);
                index.putEnd(position, locator, key);
            }
            bytes.rewind();
            return bytes;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return the block id.
     * 
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Insert a revision for {@code key} as {@code value} in {@code locator} at
     * {@code version} into this Block.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     * @throws IllegalStateException if the Block is not mutable
     */
    public Revision<L, K, V> insert(short uid, L locator, K key, V value, long version,
            Action type) throws IllegalStateException {
        write.lock();
        try {
            Preconditions.checkState(mutable,
                    "Cannot modify a block that is not mutable");
            Revision<L, K, V> revision = makeRevision(uid, locator, key, value,
                    version, type);
            revisions.add(revision);
            filter.put(revision.getLocator());
            filter.put(revision.getLocator(), revision.getKey());
            filter.put(revision.getLocator(), revision.getKey(),
                    revision.getValue()); // NOTE: The entire revision is added
                                          // to the filter so that we can
                                          // quickly verify that a revision
                                          // DOES NOT exist using
                                          // #mightContain(L,K,V) without
                                          // seeking
            size += revision.size() + 4;
            return revision;
        }
        finally {
            write.unlock();
        }
    }

    /**
     * Return {@code true} if this Block might contain revisions involving
     * {@code key} as {@code value} in {@code locator}. This method <em>may</em>
     * return a false positive, but never a false negative. If this method
     * returns {@code true}, the caller should seek for {@code key} in
     * {@code locator} and check if any of those revisions contain {@code value}
     * as a component.
     * 
     * @param locator
     * @param key
     * @param value
     * @return {@code true} if it is possible that relevant revisions exists
     */
    public boolean mightContain(L locator, K key, V value) {
        read.lock();
        try {
            return filter.mightContain(locator, key, value);
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Seek revisions that contain {@code key} in {@code locator} and append
     * them to {@code record} if it is <em>likely</em> that those revisions
     * exist in this Block.
     * 
     * @param locator
     * @param key
     * @param record
     */
    @GuardedBy("seek(Record, Byteable...)")
    public void seek(L locator, K key, Record<L, K, V> record) {
        seek(record, locator, key);
    }

    /**
     * Seek revisions that contain any key in {@code locator} and append them to
     * {@code record} if it is <em>likely</em> that those revisions exist in
     * this Block.
     * 
     * @param locator
     * @param record
     */
    @GuardedBy("seek(Record, Byteable...)")
    public void seek(L locator, Record<L, K, V> record) {
        seek(record, locator);
    }

    @Override
    public int size() {
        read.lock();
        try {
            return size;
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Flush the content to disk in a block file, sync the filter and index and
     * finally make the Block immutable.
     */
    @Override
    public void sync() {
        write.lock();
        try {
            if(size > 0) {
                Preconditions.checkState(mutable,
                        "Cannot sync a block that is not mutable");
                mutable = false;
                FileChannel channel = FileSystem.getFileChannel(file);
                channel.write(getBytes());
                channel.force(false);
                filter.sync();
                index.sync();
                FileSystem.closeFileChannel(channel);
                revisions = null; // Set to NULL so that the Set is eligible for
                                  // GC while the Block stays in memory.
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            write.unlock();
        }

    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " " + id;
    }

    /**
     * Return a dump of the revisions in the block as a String. This method
     * primarily exists for debugging using the {@link DumpToolCli} tool.
     * <p>
     * NOTE: This method will map an entire immutable block into memory, so
     * please use with caution.
     * </p>
     * 
     * @return a string dump
     */
    protected String dump() {
        read.lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Dump for " + getClass().getSimpleName() + " " + id);
            sb.append("\n");
            sb.append("------");
            sb.append("\n");
            if(mutable) {
                for (Revision<L, K, V> revision : revisions) {
                    sb.append(revision);
                    sb.append("\n");
                }
            }
            else {
                ByteBuffer bytes = FileSystem.map(file, MapMode.READ_ONLY, 0,
                        FileSystem.getFileSize(file));
                Iterator<ByteBuffer> it = ByteableCollections.iterator(bytes);
                while (it.hasNext()) {
                    Revision<L, K, V> revision = Byteables.read(it.next(),
                            xRevisionClass());
                    sb.append(revision);
                    sb.append("\n");
                }
            }
            sb.append("\n");
            return sb.toString();
        }
        finally {
            read.unlock();
        }
    }

    /**
     * Return a {@link Revision} for {@code key} as {@code value} in
     * {@code locator} at {@code version}.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     * @return the Revision
     */
    protected abstract Revision<L, K, V> makeRevision(short uid, L locator, K key,
            V value, long version, Action type);

    /**
     * Return the class of the {@code revision} type.
     * 
     * @return the revision class
     */
    protected abstract Class<? extends Revision<L, K, V>> xRevisionClass();

    /**
     * Seek revisions that contain components from {@code byteables} and append
     * them to {@code record}. The seek will be perform in memory iff this block
     * is mutable, otherwise, the seek happens on disk.
     * 
     * @param record
     * @param byteables
     */
    private void seek(Record<L, K, V> record, Byteable... byteables) {
        read.lock();
        try {
            if(filter.mightContain(byteables)) {
                if(mutable) {
                    Iterator<Revision<L, K, V>> it = revisions.iterator();
                    boolean processing = false; // Since the revisions are
                                                // sorted, I can toggle this
                                                // flag on once I reach a
                                                // revision that I care about so
                                                // that I can break out of the
                                                // loop once I reach a revision
                                                // I don't care about again.
                    boolean checkSecond = byteables.length > 1;
                    while (it.hasNext()) {
                        Revision<L, K, V> revision = it.next();
                        if(revision.getLocator().equals(byteables[0])
                                && ((checkSecond && revision.getKey().equals(
                                        byteables[1])) || !checkSecond)) {
                            processing = true;
                            record.append(revision);
                        }
                        else if(processing) {
                            break;
                        }
                    }
                }
                else {
                    int start = index.getStart(byteables);
                    int length = index.getEnd(byteables) - (start - 1);
                    if(start != BlockIndex.NO_ENTRY && length > 0) {
                        ByteBuffer bytes = FileSystem.map(file,
                                MapMode.READ_ONLY, start, length);
                        Iterator<ByteBuffer> it = ByteableCollections
                                .iterator(bytes);
                        while (it.hasNext()) {
                            Revision<L, K, V> revision = Byteables.read(
                                    it.next(), xRevisionClass());
                            record.append(revision);
                        }
                    }
                }
            }
        }
        finally {
            read.unlock();
        }
    }

    /**
     * A Comparator that sorts Revisions in a block. The sort order is
     * {@code locator} followed by {@code key} followed by {@code version}.
     * 
     * @author jnelson
     */
    @SuppressWarnings("rawtypes")
    private enum Sorter implements Comparator<Revision> {
        INSTANCE;

        /**
         * Sorts by locator followed by key followed by version.
         */
        @Override
        @SuppressWarnings("unchecked")
        public int compare(Revision o1, Revision o2) {
            int order;
            return (order = o1.getLocator().compareTo(o2.getLocator())) != 0 ? order
                    : ((order = o1.getKey().compareTo(o2.getKey())) != 0 ? order
                            : (order = Longs.compare(o1.getVersion(),
                                    o2.getVersion())) != 0 ? order : o1
                                    .getValue().compareTo(o2.getValue()));
        }

    }
}
