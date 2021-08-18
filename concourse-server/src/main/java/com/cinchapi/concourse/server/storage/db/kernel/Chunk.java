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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.Locks;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Itemizable;
import com.cinchapi.concourse.server.io.TransferableByteSequence;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

/**
 * <p>
 * A {@link Chunk} is one of several sorted collections of {@link Revision
 * Revisions} that exist within a {@link Segment} to store data in various
 * formats that are optimized for different operations.
 * </p>
 * <p>
 * When initially {@link Chunk#Chunk(BloomFilter) created}, a {@link Chunk}
 * resides solely in memory and is able to
 * {@link #insert(Byteable, Byteable, Byteable, long, Action) insert} new
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
    private final SoftReference<SortedMultiset<Revision<L, K, V>>> $revisions;

    /**
     * A flag that indicates if this {@link Chunk} can be
     * {@link #freeze(Path, long) frozen} even if its empty.
     */
    private final boolean allowEmptyFlush = this instanceof CorpusChunk;

    @Nullable
    private ByteBuffer bytes;

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
     * A running count of the number of {@link #revisions} that have been
     * {@link #insert(Byteable, Byteable, Byteable, long, Action) inserted} into
     * a {@link #mutable} {@link Chunk}.
     * <p>
     * This value is {@code null} when this {@link Chunk} is no longer
     * {@link #mutable}.
     * </p>
     */
    @Nullable
    private AtomicInteger revisionCount;

    /**
     * A collection that contains all the Revisions that have been
     * {@link #insert(Byteable, Byteable, Byteable, long, Action) inserted} into
     * the {@link Chunk}. This collection is sorted on the fly as elements are
     * inserted.
     * <p>
     * This collection is only maintained for a {@link #mutable} {@link Chunk}.
     * A {@link Chunk} that is {@link #freeze() frozen} and subsequently reads
     * from a {@link #file} does not rely on this collection at all.
     * </p>
     */
    @Nullable
    private SortedMultiset<Revision<L, K, V>> revisions;

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
     * {@link #insert(Byteable, Byteable, Byteable, long, Action) writes}
     * triggered by a
     * {@link Segment#acquire(com.cinchapi.concourse.server.storage.temp.Write, com.cinchapi.concourse.server.concurrent.AwaitableExecutorService)
     * Segment transfer}
     */
    private final ReadLock segmentReadLock;

    /**
     * Construct a new instance.
     * 
     * @param idgen
     * @param mutable
     * @param filter
     */
    protected Chunk(@Nullable Segment segment, BloomFilter filter) {
        super();
        this.segment = segment;
        this.filter = filter;
        this.file = null;
        this.length = -1;
        this.manifest = null;
        this.revisions = createBackingStore(Sorter.INSTANCE);
        this.$revisions = new SoftReference<SortedMultiset<Revision<L, K, V>>>(
                revisions);
        this.revisionCount = new AtomicInteger(0);
        this.segmentReadLock = segment != null ? segment.readLock
                : Locks.noOpReadLock();
    }

    /**
     * Load an existing instance.
     * 
     * @param idgen
     * @param file
     * @param position
     * @param length
     * @param filter
     * @param manifest
     */
    protected Chunk(@Nullable Segment segment, Path file, long position,
            long length, BloomFilter filter, Manifest manifest) {
        super(file, position, length);
        Preconditions.checkNotNull(filter);
        Preconditions.checkNotNull(manifest);
        this.segment = segment;
        this.filter = filter;
        this.file = file;
        this.length = length;
        this.manifest = manifest;
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
     * Return this {@link Chunk Chunk's} id.
     * 
     * @return the {@link Chunk} id
     */
    public String id() {
        return segment != null ? segment.id()
                : Integer.toString(System.identityHashCode(this));
    }

    /**
     * Insert a revision for {@code key} as {@code value} in {@code locator} at
     * {@code version} into this {@link Chunk}.
     * 
     * @param locator
     * @param key
     * @param value
     * @param version
     * @param type
     * @throws IllegalStateException if the {@link Chunk} is not mutable
     */
    public Revision<L, K, V> insert(L locator, K key, V value, long version,
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
                SortedMultiset<Revision<L, K, V>> revisions = $revisions != null
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
                    int start = manifest.getStart(composite);
                    int length = manifest.getEnd(composite) - (start - 1);
                    if(start != Manifest.NO_ENTRY && length > 0) {
                        ByteBuffer bytes = FileSystem.map(file,
                                MapMode.READ_ONLY, position() + start, length);
                        Iterator<ByteBuffer> it = ByteableCollections
                                .iterator(bytes);
                        while (it.hasNext()) {
                            Revision<L, K, V> revision = Byteables
                                    .read(it.next(), xRevisionClass());
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
            Locks.unlockIfCondition(segmentReadLock, mutable);
        }
    }

    @Override
    public String toString() {
        return AnyStrings.format("{} {}", getClass().getSimpleName(), id());
    }

    /**
     * Return the backing store to hold revisions that are placed in this
     * {@link Chunk}.
     * This is only relevant to use when the {@link Chunk} is {@link #mutable}
     * and not yet persisted to disk.
     * <p>
     * If this {@link Chunk} is to be {@link #concurrent} then override this
     * method and return a Concurrent Multiset.
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
        this.revisions = null;
        this.revisionCount = null;
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
     * {@link #insert(Byteable, Byteable, Byteable, long, Action)} without
     * locking. Only call this method directly if the {@link Chunk} is
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
    protected final Revision<L, K, V> insertUnsafe(L locator, K key, V value,
            long version, Action type) throws IllegalStateException {
        Preconditions.checkState(isMutable(),
                "Cannot modify an immutable chunk");
        Revision<L, K, V> revision = makeRevision(locator, key, value, version,
                type);
        revisions.add(revision);
        revisionCount.incrementAndGet();
        filter.put(Composite.create(revision.getLocator()));
        filter.put(Composite.create(revision.getLocator(), revision.getKey()));
        filter.put(Composite.create(revision.getLocator(), revision.getKey(),
                revision.getValue())); // NOTE: The entire revision is added
                                       // to the filter so that we can
                                       // quickly verify that a revision
                                       // DOES NOT exist using
                                       // #mightContain(L,K,V) without
                                       // seeking
        incrementLengthBy(revision.size() + 4);
        manifest = null;
        bytes = null;
        return revision;
    }

    /**
     * Internal implementation to return the {@link #length()} of this
     * {@link Chunk} without grabbing any locks.
     * 
     * @return the length
     */
    protected abstract long lengthUnsafe();

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
                int position = 0;
                boolean populated = false;
                for (Revision<L, K, V> revision : revisions) {
                    populated = true;
                    sink.putInt(revision.size());
                    revision.copyTo(sink);
                    position = ((int) sink.position()) - revision.size() - 4;
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
                            // first!)
                            // and we need to record the end index.
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
                    position = ((int) sink.position()) - 1;
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
                            .streamingIterator(file, position(), length,
                                    GlobalState.BUFFER_PAGE_SIZE);

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
            return ComparisonChain.start()
                    .compare(o1.getLocator(), o2.getLocator())
                    .compare(o1.getKey(), o2.getKey())
                    .compare(o1.getVersion(), o2.getVersion())
                    .compare(o1.getValue(), o2.getValue()).result();
        }

    }

}
