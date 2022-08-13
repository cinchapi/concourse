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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.Locks;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.Itemizable;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.kernel.Manifest.Range;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.lib.offheap.collect.OffHeapSortedSet;
import com.cinchapi.lib.offheap.io.Serializer;
import com.cinchapi.lib.offheap.memory.OffHeapMemory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 * <p>
 * A {@link Chunk} is one of several sorted collections of {@link Revision
 * Revisions} that exist within a {@link Segment} to store data in various
 * formats that are optimized for different operations.
 * </p>
 * <p>
 * When initially {@link Chunk#Chunk(BloomFilter) created}, a {@link Chunk}
 * resides solely in memory and is able to
 * {@link #insert(Byteable, Byteable, Byteable, long, Action) insert}
 * new
 * {@link Revision revisions} (this corresponds to the
 * {@link Segment#acquire(com.cinchapi.concourse.server.storage.temp.Write, com.cinchapi.concourse.server.concurrent.AwaitableExecutorService)}
 * functionality), which are sorted on the fly. Once the {@link Chunk} is
 * {@link #freeze(Path, long) frozen} (happens when its parent {@link Segment}
 * is
 * {@link Segment#sync(com.cinchapi.concourse.server.concurrent.AwaitableExecutorService)
 * synced to disk}) it becomes immutable and all lookups eventually become disk
 * based. This means that writing to a {@link Chunk} never incurs any random
 * disk I/O and reading from a {@link #freeze(Path, long) frozen} chunk only
 * loads into memory as much data from disk as necessary to support each
 * individual operation.
 * </p>
 * <p>
 * The parent {@link Segment} associates each {@link Chunk} with a
 * {@link BloomFilter} and {@link Manifest} to make disk-based lookups more
 * efficient. The {@link BloomFilter} is used to test whether it is possible
 * that a {@link Revision} with certain attributes can exist in the
 * {@link Chunk}. The {@link Manifest} is then used to find the exact start and
 * end positions for {@link Revision Revisions} that are relevant to an
 * operation. This means that reading from a {@link Chunk} never incurs any
 * unnecessary disk I/O.
 * </p>
 * 
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public abstract class Chunk<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends TransferableByteSequence implements
        Iterable<Revision<L, K, V>>,
        Itemizable {

    /**
     * A soft reference to the {@link #revisions} that <em>may</em> stay in
     * memory after the {@link Chunk} has been synced. The GC is encouraged to
     * clear this reference in response to memory pressure at which point disk
     * seeks will be performed in the {@link #seek(Record, Byteable...)} method.
     */
    private final SoftReference<SortedSet<Revision<L, K, V>>> $revisions;

    /**
     * A flag that indicates if this {@link Chunk} can be
     * {@link #freeze(Path, long) frozen} even if its empty.
     */
    private final boolean allowEmptyFlush = this instanceof CorpusChunk;

    /**
     * The bytes for all the {@link #revisions} in a {@link #isMutable()
     * mutable} {@link Chunk} that are generated alongside the {@link #manifest}
     * if the {@link #length} is less than {@link Integer#MAX_VALUE}.
     * <p>
     * If this value is not {@code null}, it is cleared when new data is
     * {@link #insert(Byteable, Byteable, Byteable, long, Action)
     * inserted}.
     * </p>
     */
    @Nullable
    private ByteBuffer bytes;

    /**
     * Additional {@link Runnable actions} that are executed when the
     * {@link Chunk} is {@link #free() freed}.
     */
    private final List<Runnable> cleaners = new ArrayList<>(1);

    /**
     * A fixed size filter that is used to test whether elements are contained
     * in the {@link Chunk} without actually looking through the {@link Chunk}.
     */
    private final BloomFilter filter;

    /**
     * The known length of this {@link Chunk}.
     * <p>
     * This value is {@code null} unless the {@link Chunk} has been
     * {@link #Chunk(Path, long, long, BloomFilter, Manifest) loaded} or
     * {@link #transfer(Path, long) transferred}. If the value is {@code null},
     * the subclass keeps track within the {@link #lengthUnsafe()} method.
     * </p>
     */
    @Nullable
    private long length;

    /**
     * The {@link Manifest} that provides the exact location of data when
     * {@link #seek(Record, Byteable...) seeking} from disk.
     */
    @Nullable
    private Manifest manifest;

    /**
     * A collection that shadows {@link Segment#objects()} to handle
     * {@link #deduplicate(Byteable) deduplication}.
     */
    @Nullable
    private Map<Byteable, Byteable> objects;

    /**
     * A running count of the number of {@link #revisions} that have been
     * {@link #insert(Byteable, Byteable, Byteable, long, Action)
     * inserted} into a {@link #mutable} {@link Chunk}.
     * <p>
     * This value is {@code null} when this {@link Chunk} is no longer
     * {@link #mutable}.
     * </p>
     */
    @Nullable
    private AtomicInteger revisionCount;

    /**
     * A collection that contains all the Revisions that have been
     * {@link #insert(Byteable, Byteable, Byteable, long, Action)
     * inserted} into the {@link Chunk}. This collection is sorted on the fly as
     * elements are inserted.
     * <p>
     * This collection is only maintained for a {@link #mutable} {@link Chunk}.
     * A {@link Chunk} that is {@link #freeze() frozen} and subsequently reads
     * from a {@link #file} does not rely on this collection at all.
     * </p>
     */
    /*
     * IMPLEMENTATION NOTE
     * -------------------
     * Even though Revisions with the same locator, key and value are considered
     * "equals", we use a Set instead of a Set because the specially
     * designed SORTER leverages the unique version associated with each
     * Revision to determine equality. This technically breaks the contract that
     * Set wants between a comparator and #equals, but it practically works.
     */
    @Nullable
    private SortedSet<Revision<L, K, V>> revisions;

    /**
     * A reference to the {@link Segment} to which this {@link Chunk} is
     * maintained.
     */
    @Nullable
    private final Segment segment;

    /**
     * A reference to the {@link Segment Segment's} read lock that is necessary
     * to prevent inconsistent reads across the various {@link Chunk chunks} due
     * to the non-atomic asynchronous
     * {@link #insert(Byteable, Byteable, Byteable, long, Action)
     * writes}
     * triggered by a
     * {@link Segment#acquire(com.cinchapi.concourse.server.storage.temp.Write, com.cinchapi.concourse.server.concurrent.AwaitableExecutorService)
     * Segment transfer}
     */
    private final ReadLock segmentReadLock;

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param filter
     */
    protected Chunk(@Nullable Segment segment, BloomFilter filter) {
        super();
        this.segment = segment;
        this.filter = filter;
        this.length = -1;
        this.manifest = null;
        this.objects = segment != null ? segment.objects() : null;
        while (objects == null) {
            Logger.warn(
                    "{} is using a standalone object pool instead of one shared among %s",
                    this, segment);
            objects = new ConcurrentHashMap<>();
        }
        this.revisions = createBackingStore(Sorter.INSTANCE);
        this.$revisions = new SoftReference<SortedSet<Revision<L, K, V>>>(
                revisions);
        this.revisionCount = new AtomicInteger(0);
        this.segmentReadLock = segment != null ? segment.readLock
                : Locks.noOpReadLock();
    }

    /**
     * Load an existing instance.
     * 
     * @param segment
     * @param file
     * @param channel
     * @param position
     * @param length
     * @param filter
     * @param manifest
     */
    protected Chunk(@Nullable Segment segment, Path file, FileChannel channel,
            long position, long length, BloomFilter filter, Manifest manifest) {
        super(file, channel, position, length);
        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(manifest);
        this.segment = segment;
        this.filter = filter;
        this.length = length;
        this.manifest = manifest;
        this.objects = null;
        this.revisions = null;
        this.$revisions = null;
        this.revisionCount = null;
        this.segmentReadLock = segment != null ? segment.readLock
                : Locks.noOpReadLock();
    }

    @Override
    public long count() {
        if(revisionCount != null) {
            return revisionCount.get();
        }
        else {
            throw new IllegalStateException("Cannot count an immutable chunk");
        }
    }

    /**
     * Return a dump of the revisions in the {@link Chunk} as a String. This
     * method primarily exists for debugging using the {@link ManageDataCli}
     * tool.
     * 
     * @return a string dump
     */
    public String dump() {
        boolean mutable = isMutable();
        Locks.lockIfCondition(read, mutable);
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Dump for " + this);
            sb.append("\n");
            sb.append("------");
            sb.append("\n");
            for (Revision<L, K, V> revision : revisions()) {
                sb.append(revision);
                sb.append("\n");
            }
            sb.append("\n");
            return sb.toString();
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Chunk) {
            List<Revision<L, K, V>> mine = Lists.newArrayList();
            List<Revision<?, ?, ?>> theirs = Lists.newArrayList();
            for (Revision<L, K, V> revision : revisions()) {
                mine.add(revision);
            }
            for (Revision<?, ?, ?> revision : ((Chunk<?, ?, ?>) obj)
                    .revisions()) {
                theirs.add(revision);
            }
            return mine.equals(theirs);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return revisions().hashCode();
    }

    /**
     * Insert a revision for {@code key} as {@code value} in {@code locator} at
     * {@code version} to this {@link Chunk}.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     * @throws IllegalStateException if the {@link Chunk} is not mutable
     */
    public Artifact<L, K, V> insert(L locator, K key, V value, long version,
            Action type) throws IllegalStateException {
        boolean mutable = isMutable();
        Locks.lockIfCondition(write, mutable);
        try {
            return insertUnsafe(locator, key, value, version, type);
        }
        finally {
            Locks.unlockIfCondition(write, mutable);
        }
    }

    @Override
    public Iterator<Revision<L, K, V>> iterator() {
        return revisions().iterator();
    }

    @Override
    public long length() {
        boolean mutable = isMutable();
        Locks.lockIfCondition(read, mutable);
        try {
            if(length >= 0) {
                return length;
            }
            else {
                return bytes == null ? lengthUnsafe() : bytes.capacity();
            }
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    /**
     * Return {@code true} if this {@link Chunk} might contain revisions
     * involving {@code key} as {@code value} in {@code locator}. This method
     * <em>may</em> return a false positive, but never a false negative. If this
     * method returns {@code true}, the caller should seek for {@code key} in
     * {@code locator} and check if any of those revisions contain {@code value}
     * as a component.
     * 
     * @param locator
     * @param key
     * @param value
     * @return {@code true} if it is possible that relevant revisions exists
     */
    public boolean mightContain(Composite composite) {
        boolean mutable = isMutable();
        Locks.lockIfCondition(read, mutable);
        try {
            return filter.mightContain(composite);
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    /**
     * If it possible that they exist, look for any {@link Revision revisions}
     * that match the {@code composite} and {@link Record#append(Revision)
     * append} hem to the {@code record}.
     * 
     * @param composite
     * @param record
     */
    public final void seek(Composite composite, Record<L, K, V> record) {
        boolean mutable = isMutable();
        Locks.lockIfCondition(segmentReadLock, mutable);
        Locks.lockIfCondition(read, mutable);
        try {
            if(filter.mightContain(composite)) {
                SortedSet<Revision<L, K, V>> revisions = $revisions != null
                        ? $revisions.get()
                        : null;
                if(revisions != null) {
                    Iterator<Revision<L, K, V>> it = revisions.iterator();
                    boolean processing = false; // Since the revisions are
                                                // sorted, I can toggle this
                                                // flag on once I reach a
                                                // revision that I care about so
                                                // that I can break out of the
                                                // loop once I reach a revision
                                                // I don't care about again.
                    boolean checkSecond = composite.parts().length > 1;
                    while (it.hasNext()) {
                        Revision<L, K, V> revision = it.next();
                        if(revision.getLocator().equals(composite.parts()[0])
                                && ((checkSecond && revision.getKey()
                                        .equals(composite.parts()[1]))
                                        || !checkSecond)) {
                            processing = true;
                            record.append(revision);
                        }
                        else if(processing) {
                            break;
                        }
                    }
                }
                else {
                    Range range = manifest.lookup(composite);
                    long start = range.start();
                    long length = range.end() - (start - 1);
                    if(start != Manifest.NO_ENTRY && length > 0) {
                        Iterator<ByteBuffer> it = ByteableCollections.stream(
                                channel(), position() + start, length,
                                GlobalState.DISK_READ_BUFFER_SIZE);
                        while (it.hasNext()) {
                            Revision<L, K, V> revision = Byteables
                                    .read(it.next(), xRevisionClass());
                            Logger.debug(
                                    "Attempting to append {} from {} to {}",
                                    revision, this, record);
                            record.append(revision);
                        }
                    }
                }
            }
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
            Locks.unlockIfCondition(segmentReadLock, mutable);
        }
    }

    @Override
    public String toString() {
        return AnyStrings.format("{} of {}", getClass().getSimpleName(),
                segment);
    }

    /**
     * Return the backing store to hold revisions that are placed in this
     * {@link Chunk}. This is only relevant to use when the {@link Chunk} is
     * {@link #mutable} and not yet persisted to disk.
     * <p>
     * If this {@link Chunk} is to be {@link #concurrent} then override this
     * method and return a Concurrent Set.
     * </p>
     * 
     * @param comparator
     * @return the backing store
     */
    @SuppressWarnings("rawtypes")
    protected SortedSet<Revision<L, K, V>> createBackingStore(
            Comparator<Revision> comparator) {
        return new TreeSet<>(comparator);
    }

    /**
     * Return the off heap backing store to hold revisions that are placed in
     * this {@link Chunk}. This is only relevant to use when the {@link Chunk}
     * is {@link #mutable}, {@link #shift(OffHeapMemory) shifted} and not yet
     * persisted to disk.
     * <p>
     * If this {@link Chunk} is to be {@link #concurrent} then override this
     * method and return a Concurrent Set.
     * </p>
     * 
     * @param memory
     * @param comparator
     * @param serializer
     * @return the backing store
     */
    protected OffHeapSortedSet<Revision<L, K, V>> createOffHeapBackingStore(
            OffHeapMemory memory, Comparator<Revision<L, K, V>> comparator,
            Serializer<Revision<L, K, V>> serializer) {
        return new OffHeapSortedSet<>(memory, comparator, serializer);
    }

    /**
     * Return this {@link Chunk Chunk's} {@link BloomFilter}.
     * 
     * @return the {@link #filter}
     */
    protected BloomFilter filter() {
        return filter;
    }

    @Override
    protected void flush(ByteSink sink) {
        // NOTE: The parent #segment is responsible for flushing the #index and
        // #manifest.
        this.length = length();
        if(length == 0 && allowEmptyFlush) {
            return;
        }
        else if(length == 0) {
            throw new IllegalStateException(
                    "Cannot flush " + this + " because it is empty");
        }
        else {
            manifest();
            if(bytes == null) {
                // length > Integer#MAX_VALUE so the #bytes were not
                // gathered while creating the #manifest
                Logger.warn("Flushing {} requires more than {} bytes", this,
                        Integer.MAX_VALUE);
                for (Revision<L, K, V> revision : revisions) {
                    sink.putInt(revision.size());
                    revision.copyTo(sink);
                }
            }
            else {
                sink.put(bytes);
            }
        }
    }

    @Override
    protected void free() {
        Logger.debug("Freeing memory in {}", this);
        this.objects = null;
        this.revisions = null;
        this.revisionCount = null;
        this.bytes = null;
        for (Runnable cleaner : cleaners) {
            cleaner.run();
        }
    }

    /**
     * Increment the {@link #lengthUnsafe()} of this {@link Chunk} by
     * {@code delta} in a manner that accounts for whether the {@link Chunk} is
     * concurrent or not.
     * 
     * @param delta
     */
    protected abstract void incrementLengthBy(int delta);

    /**
     * {@link #insert(Byteable, Byteable, Byteable, long, Action)}
     * without locking. Only call this method directly if the {@link Chunk} is
     * {@link #concurrent}.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     * @return the inserted {@link Revision}
     * @throws IllegalStateException
     */
    protected final Artifact<L, K, V> insertUnsafe(L locator, K key, V value,
            long version, Action type) throws IllegalStateException {
        Preconditions.checkState(isMutable(),
                "Cannot modify an immutable chunk");
        //@formatter:off
        locator = deduplicate(locator);
        key     = deduplicate(key);
        value   = deduplicate(value);
        //@formatter:on
        Revision<L, K, V> revision = makeRevision(locator, key, value, version,
                type);
        revisions.add(revision);
        revisionCount.incrementAndGet();
        // @formatter:off
        Composite[] composites = Array.containing(
                Composite.create(revision.getLocator()),
                Composite.create(revision.getLocator(), revision.getKey()),
                Composite.create(revision.getLocator(), revision.getKey(),
                        revision.getValue()) // NOTE: The entire revision is added
                                             // to the filter so that we can
                                             // quickly verify that a revision
                                             // DOES NOT exist using
                                             // #mightContain(L,K,V) without
                                             // seeking
        );
        // @formatter:on
        for (Composite composite : composites) {
            filter.put(composite);
        }
        incrementLengthBy(revision.size() + 4);
        manifest = null;
        bytes = null;
        return makeArtifact(revision, composites);
    }

    /**
     * Internal implementation to return the {@link #length()} of this
     * {@link Chunk} without grabbing any locks.
     * 
     * @return the length
     */
    protected abstract long lengthUnsafe();

    /**
     * Return an {@link Artifact} that contains {@code revision} and
     * {@code composites}.
     * 
     * @param revision
     * @param composites
     * @return the {@link Artifact}
     */
    protected abstract Artifact<L, K, V> makeArtifact(
            Revision<L, K, V> revision, Composite[] composites);

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
    protected abstract Revision<L, K, V> makeRevision(L locator, K key, V value,
            long version, Action type);

    /**
     * Return this {@link Chunk Chunk's} {@link Manifest}, generating it, if
     * necessary.
     * <p>
     * NOTE: The positions recorded in the {@link Manifest} are relative to
     * the {@link #position} that is recorded when this {@link Chunk} is
     * {@link #transfer(Path, long) transferred}.
     * </p>
     * 
     * @return the {@link #manifest}
     */
    protected Manifest manifest() {
        boolean mutable = isMutable();
        Locks.lockIfCondition(read, mutable);
        try {
            while (manifest == null) {
                long length = lengthUnsafe();
                bytes = length <= Integer.MAX_VALUE
                        ? ByteBuffer.allocate((int) length)
                        : null;
                ByteSink sink = bytes != null ? ByteSink.to(bytes)
                        : ByteSink.toDevNull();
                Manifest manifest = Manifest.create(revisionCount.get());
                L locator = null;
                K key = null;
                long position = 0;
                boolean populated = false;
                for (Revision<L, K, V> revision : revisions) {
                    populated = true;
                    sink.putInt(revision.size());
                    revision.copyTo(sink);
                    position = sink.position() - revision.size() - 4;
                    /*
                     * States that trigger this condition to be true:
                     * 1. This is the first locator we've seen
                     * 2. This locator is different than the last one we've seen
                     */
                    if(locator == null
                            || !locator.equals(revision.getLocator())) {
                        manifest.putStart(position, revision.getLocator());
                        if(locator != null) {
                            // There was a locator before us (we are not the
                            // first!) and we need to record the end index.
                            manifest.putEnd(position - 1, locator);
                        }
                    }
                    /*
                     * NOTE: IF key == null, then it must be the case that
                     * locator == null since they are set at the same time.
                     * Therefore we do not need to explicitly check for that
                     * condition below
                     * 
                     * States that trigger this condition to be true:
                     * 1. This is the first key we've seen
                     * 2. This key is different than the last one we've seen
                     * (regardless of whether the locator is different or the
                     * same!)
                     * 3. This key is the same as the last one we've seen, but
                     * the locator is different.
                     */
                    if(key == null || !key.equals(revision.getKey())
                            || !locator.equals(revision.getLocator())) {
                        manifest.putStart(position, revision.getLocator(),
                                revision.getKey());
                        if(key != null) {
                            // There was a locator, key before us (we are not
                            // the first!) and we need to record the end index.
                            manifest.putEnd(position - 1, locator, key);
                        }
                    }
                    locator = revision.getLocator();
                    key = revision.getKey();
                }
                if(populated) {
                    position = sink.position() - 1;
                    manifest.putEnd(position, locator);
                    manifest.putEnd(position, locator, key);
                }
                this.manifest = manifest;
                if(bytes != null) {
                    bytes.flip();
                }
            }
            return manifest;
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    /**
     * Return this {@link Chunk Chunk's} parent {@link Segment}.
     * 
     * @return the parent {@link Segment}
     */
    @Nullable
    protected Segment segment() {
        return segment;
    }

    /**
     * Return the class of the {@code revision} type.
     * 
     * @return the revision class
     */
    protected abstract Class<? extends Revision<L, K, V>> xRevisionClass();

    /**
     * Shift this {@link Chunk} to store its data in the provided
     * {@link OffHeapMemory memory} segment, as opposed to the Java heap.
     * 
     * @param memory
     */
    final void shift(OffHeapMemory memory) {
        Locks.lockIfCondition(write, isMutable());
        try {
            Preconditions.checkState(isMutable(),
                    "Cannot shift an immutable Chunk to OffHeapMemory");
            Serializer<Revision<L, K, V>> serializer = new Serializer<Revision<L, K, V>>() {

                @Override
                public Revision<L, K, V> deserialize(OffHeapMemory memory) {
                    return Byteables.read(memory, xRevisionClass());
                }

                @Override
                public void serialize(Revision<L, K, V> element,
                        OffHeapMemory memory) {
                    element.copyTo(ByteSink.to(memory));
                }

                @Override
                public int sizeOf(Revision<L, K, V> element) {
                    return element.size();
                }

            };
            OffHeapSortedSet<Revision<L, K, V>> offHeapRevisions = createOffHeapBackingStore(
                    memory, (rev1, rev2) -> Sorter.INSTANCE.compare(rev1, rev2),
                    serializer);
            for (Revision<L, K, V> revision : revisions) {
                offHeapRevisions.add(revision);
            }
            revisions = offHeapRevisions;
            cleaners.add(() -> memory.free());
        }
        finally {
            Locks.unlockIfCondition(write, isMutable());
        }
    }

    /**
     * Return an object that is equal to {@code reference} if one has been
     * previously stored as either a locator, key or value. Otherwise, record
     * {@code reference} as the canonical instance for equal objects that may
     * later be seen.
     * 
     * @param ref
     * @return the deduplicated reference
     */
    @SuppressWarnings("unchecked")
    private <T extends Byteable> T deduplicate(T ref) {
        // TODO: potentially handle size issues by catching exception and no
        // longer trying to deduplicate?
        Preconditions.checkNotNull(ref);
        if(objects != null) {
            ref = (T) objects.computeIfAbsent(ref, $ref -> $ref);
        }
        return ref;
    }

    /**
     * Return an {@link Iterable} over this {@link Chunk}'s {@link Revision
     * revisions}.
     * <p>
     * Unless the {@link {@link Chunk}} is {@link #mutable} and has its
     * {@link #revisions} in memory, this method will stream the data directly
     * from disk in an efficient manner.
     * </p>
     * <p>
     * If this {@link Chunk} is {@link #mutable}, the returned iterable is a
     * live-view.
     * </p>
     * 
     * @return an iterable over the revisions
     */
    private Iterable<Revision<L, K, V>> revisions() {
        if(revisions != null) {
            return revisions;
        }
        else if($revisions != null && $revisions.get() != null) {
            return $revisions.get();
        }
        else {
            return () -> {
                // Incrementally stream the revisions from disk

                return new Iterator<Revision<L, K, V>>() {

                    private final Iterator<ByteBuffer> it = ByteableCollections
                            .stream(file(), position(), length,
                                    GlobalState.DISK_READ_BUFFER_SIZE);

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

            };
        }
    }

    /**
     * A Comparator that sorts Revisions in a {@link Chunk}. The sort order is
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
            // @formatter:off
            return ComparisonChain.start()
                    .compare(o1.getLocator(), o2.getLocator())
                    .compare(o1.getKey(), o2.getKey())
                    .compare(o1.getVersion(), o2.getVersion())
                    .compare(o1.getValue(), o2.getValue())
                    .compare(o1.stamp(), o2.stamp())
                    .result();
            // @formatter:on
        }

    }

}
