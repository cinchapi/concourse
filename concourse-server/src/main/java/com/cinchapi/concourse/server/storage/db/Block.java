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
package com.cinchapi.concourse.server.storage.db;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.Locks;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Syncable;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

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
 * @author Jeff Nelson
 */
@ThreadSafe
@PackagePrivate
abstract class Block<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> implements
        Byteable,
        Syncable,
        Iterable<Revision<L, K, V>> {

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
     * The extension for the {@link BloomFilter} file.
     */
    private static final String FILTER_NAME_EXTENSION = ".fltr";

    /**
     * The extension for the {@link BlockIndex} file.
     */
    private static final String INDEX_NAME_EXTENSION = ".indx";

    /**
     * The extension for the block file.
     */
    @PackagePrivate
    static final String BLOCK_NAME_EXTENSION = ".blk";

    /**
     * The location of the block file.
     */
    private final String file;

    /**
     * A fixed size filter that is used to test whether elements are contained
     * in the Block without actually looking through the Block.
     */
    private BloomFilter filter;

    /**
     * The unique id for the block. Each component of the block is named after
     * the id. It is assumed that block ids should be assigned in atomically
     * increasing order (i.e. a timestamp).
     */
    private final String id;

    /**
     * A flag that indicates whether we should ignore (and not log a warning) if
     * an attempt is made to sync an empty block.
     */
    private final boolean ignoreEmptySync;

    /**
     * The index to determine which bytes in the block pertain to a locator or
     * locator/key pair.
     */
    private final BlockIndex index; // Since the index is only used for
                                    // immutable blocks, it is only populated
                                    // during the call to #getBytes()

    /**
     * The master lock for {@link #write} and {@link #read}. DO NOT use this
     * lock directly.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * A collection that contains all the Revisions that have been inserted into
     * the Block. This collection is sorted on the fly as elements are inserted.
     * This collection is only maintained for a mutable Block. A Block that is
     * synced and subsequently read from disk does not rely on this collection
     * at all.
     */
    @Nullable
    private SortedMultiset<Revision<L, K, V>> revisions;

    /**
     * The running size of the Block. This number only refers to the size of the
     * Revisions that are stored in the block file. The size for the filter and
     * index are tracked separately.
     */
    private transient int size;

    /**
     * The size counter to use if this Block is {@link #concurrent} and uses the
     * {@link #insertUnsafe(Byteable, Byteable, Byteable, long, Action)} method.
     */
    private transient AtomicInteger atomicSize = new AtomicInteger(0);

    /**
     * A soft reference to the {@link #revisions} that <em>may</em> stay in
     * memory after the Block has been synced. The GC is encouraged to clear
     * this reference in response to memory pressure at which point disk seeks
     * will be performed in the {@link #seek(Record, Byteable...)} method.
     */
    private final SoftReference<SortedMultiset<Revision<L, K, V>>> softRevisions;

    /**
     * A hint that this Block uses the
     * {@link #insertUnsafe(Byteable, Byteable, Byteable, long, Action)} method
     * to add data without grabbing any locks. This is generally safe to do as
     * long as {@link #createBackingStore(Comparator)} returns a concurrent
     * collection that is thread safe.
     */
    protected transient boolean concurrent = false;

    /**
     * The flag that indicates whether the Block is mutable or not. A Block is
     * mutable until a call to {@link #sync()} stores it to disk.
     */
    protected transient boolean mutable;

    /**
     * A shared lock that permits many readers and no writer. Use this lock to
     * ensure that no data insert occurs while a seek is happening within the
     * Block.
     */
    protected final ReadLock read = master.readLock();

    /**
     * An exclusive lock that permits only one writer and no reader. Use this
     * lock to ensure that no seek occurs while data is being inserted into the
     * Block.
     */
    protected final WriteLock write = master.writeLock();

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
            try {
                this.filter = BloomFilter.open(directory + File.separator + id
                        + FILTER_NAME_EXTENSION);
                filter.disableThreadSafety();
            }
            catch (RuntimeException e) {
                repair(e);
            }
            this.index = BlockIndex.open(directory + File.separator + id
                    + INDEX_NAME_EXTENSION);
            this.revisions = null;
        }
        else {
            this.mutable = true;
            this.size = 0;
            this.revisions = createBackingStore(Sorter.INSTANCE);
            this.filter = BloomFilter.create(
                    (directory + File.separator + id + FILTER_NAME_EXTENSION),
                    EXPECTED_INSERTIONS);
            this.index = BlockIndex.create(directory + File.separator + id
                    + INDEX_NAME_EXTENSION, EXPECTED_INSERTIONS);
        }
        this.softRevisions = new SoftReference<SortedMultiset<Revision<L, K, V>>>(
                revisions);
        this.ignoreEmptySync = this instanceof SearchBlock;
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        Locks.lockIfCondition(read, mutable);
        try {
            L locator = null;
            K key = null;
            int position = 0;
            boolean populated = false;
            for (Revision<L, K, V> revision : revisions) {
                populated = true;
                buffer.putInt(revision.size());
                revision.copyTo(buffer);
                position = buffer.position() - revision.size() - 4;
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
            if(populated) {
                position = buffer.position() - 1;
                index.putEnd(position, locator);
                index.putEnd(position, locator, key);
            }
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Block) {
            // CON-83: I am intentionally making all Blocks with the same #id
            // equal, regardless of subclass type.
            return id.equals(((Block<?, ?, ?>) obj).id);
        }
        else {
            return false;
        }
    }

    @Override
    public ByteBuffer getBytes() {
        read.lock();
        try {
            ByteBuffer bytes = ByteBuffer.allocate(sizeImpl());
            copyTo(bytes);
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

    @Override
    public int hashCode() {
        return id.hashCode();
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
    public Revision<L, K, V> insert(L locator, K key, V value, long version,
            Action type) throws IllegalStateException {
        Locks.lockIfCondition(write, mutable);
        try {
            Preconditions.checkState(mutable,
                    "Cannot modify a block that is not mutable");
            Revision<L, K, V> revision = makeRevision(locator, key, value,
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
            Locks.unlockIfCondition(write, mutable);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> Use this method with extreme caution because it
     * will load all of the revisions from disk, into memory.
     * </p>
     */
    @Override
    public Iterator<Revision<L, K, V>> iterator() {
        Preconditions.checkState(!mutable, "Cannot iterate a mutable block");
        return new Iterator<Revision<L, K, V>>() {

            private final Iterator<ByteBuffer> it = ByteableCollections
                    .streamingIterator(file, GlobalState.BUFFER_PAGE_SIZE);

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Revision<L, K, V> next() {
                ByteBuffer next = it.next();
                if(next != null) {
                    return Byteables.read(next, xRevisionClass());
                }
                else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
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
        Locks.lockIfCondition(read, mutable);
        try {
            return filter.mightContain(locator, key, value);
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
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
        Locks.lockIfCondition(read, mutable);
        try {
            return sizeImpl();
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
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
            if(mutable && sizeImpl() > 0) {
                mutable = false;
                FileChannel channel = FileSystem.getFileChannel(file);
                channel.write(getBytes());
                channel.force(true);
                filter.sync();
                index.sync();
                FileSystem.closeFileChannel(channel);
                revisions = null; // Set to NULL so that the Set is eligible for
                                  // GC while the Block stays in memory.
                filter.disableThreadSafety();
            }
            else if(!mutable) {
                Logger.warn("Cannot sync a block that is not mutable: {}", id);
            }
            else if(!ignoreEmptySync) {
                Logger.warn("Cannot sync a block that is empty: {}. "
                        + "Was there an unexpected server shutdown recently?",
                        id);
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
     * Attempt to repair the Block from the symptoms of the specified exception.
     * Generally speaking, a repair is only possible if the exception pertains
     * to the metadata (e.g. filter or index) and not the actual block data.
     * <p>
     * If a repair is not possible, then the input exception is re-thrown
     * </p>
     * 
     * @param e - the {@link RuntimeException} that was caught indicates what
     *            error needs to be repaired.
     */
    private void repair(RuntimeException e) {
        if(e.getCause() != null
                && (e.getCause() instanceof EOFException || e.getCause() instanceof StreamCorruptedException)) {
            String target = file.replace(BLOCK_NAME_EXTENSION,
                    FILTER_NAME_EXTENSION);
            String backup = target + ".bak";
            FileSystem.copyBytes(target, backup);
            FileSystem.deleteFile(target);
            filter = BloomFilter.create(target, EXPECTED_INSERTIONS);
            MappedByteBuffer bytes = FileSystem.map(file, MapMode.READ_ONLY, 0,
                    FileSystem.getFileSize(file));
            Iterator<ByteBuffer> it = ByteableCollections.iterator(bytes);
            while (it.hasNext()) {
                Revision<L, K, V> revision = Byteables.read(it.next(),
                        xRevisionClass());
                filter.put(revision.getLocator());
                filter.put(revision.getLocator(), revision.getKey());
                filter.put(revision.getLocator(), revision.getKey(),
                        revision.getValue());
            }
            filter.sync();
            FileSystem.deleteFile(backup);
            Logger.warn("Found and repaired a corrupted bloom "
                    + "filter for {} {}", this.getClass().getSimpleName(), id);
            FileSystem.unmap(bytes);
        }
        else {
            throw e;
        }
    }

    /**
     * Seek revisions that contain components from {@code byteables} and append
     * them to {@code record}. The seek will be perform in memory iff this block
     * is mutable, otherwise, the seek happens on disk.
     * 
     * @param record
     * @param byteables
     */
    private void seek(Record<L, K, V> record, Byteable... byteables) {
        Locks.lockIfCondition(read, mutable);
        try {
            if(filter.mightContain(byteables)) {
                SortedMultiset<Revision<L, K, V>> revisions = softRevisions
                        .get();
                if(revisions != null) {
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
                            Logger.debug("Attempting to append {} from {} to "
                                    + "{}", revision, this, record);
                            record.append(revision);
                        }
                    }
                }
            }
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    /**
     * Internal implementation to return size of this Block without grabbing any
     * locks.
     * 
     * @return the size
     */
    private int sizeImpl() {
        return concurrent ? atomicSize.get() : size;
    }

    /**
     * Return the backing store to hold revisions that are placed in this Block.
     * This is only relevant to use when the Block is {@link #mutable} and not
     * yet persisted to disk.
     * <p>
     * If this Block is to be {@link #concurrent} then override this method and
     * return a Concurrent Multiset.
     * </p>
     * 
     * @param comparator
     * @return the backing store
     */
    @SuppressWarnings("rawtypes")
    protected SortedMultiset<Revision<L, K, V>> createBackingStore(
            Comparator<Revision> comparator) {
        return TreeMultiset.create(comparator);
    }

    /**
     * Return a dump of the revisions in the block as a String. This method
     * primarily exists for debugging using the {@link ManageDataCli} tool.
     * <p>
     * NOTE: This method will map an entire immutable block into memory, so
     * please use with caution.
     * </p>
     * 
     * @return a string dump
     */
    protected String dump() {
        Locks.lockIfCondition(read, mutable);
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
            Locks.unlockIfCondition(read, mutable);
        }
    }

    protected Revision<L, K, V> insertUnsafe(L locator, K key, V value,
            long version, Action type) throws IllegalStateException {
        Preconditions.checkState(mutable,
                "Cannot modify a block that is not mutable");
        Revision<L, K, V> revision = makeRevision(locator, key, value, version,
                type);
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
        atomicSize.addAndGet(revision.size() + 4);
        return revision;

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
    protected abstract Revision<L, K, V> makeRevision(L locator, K key,
            V value, long version, Action type);

    /**
     * Return the class of the {@code revision} type.
     * 
     * @return the revision class
     */
    protected abstract Class<? extends Revision<L, K, V>> xRevisionClass();

    /**
     * A Comparator that sorts Revisions in a block. The sort order is
     * {@code locator} followed by {@code key} followed by {@code version}.
     * 
     * @author Jeff Nelson
     */
    @SuppressWarnings("rawtypes")
    private enum Sorter implements Comparator<Revision> {
        INSTANCE;

        /**
         * Sorts by locator followed by key followed by version.
         */
        @Override
        public int compare(Revision o1, Revision o2) {
            return ComparisonChain.start()
                    .compare(o1.getLocator(), o2.getLocator())
                    .compare(o1.getKey(), o2.getKey())
                    .compare(o1.getVersion(), o2.getVersion())
                    .compare(o1.getValue(), o2.getValue()).result();
        }

    }
}
