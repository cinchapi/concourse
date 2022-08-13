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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.concurrent.Locks;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Itemizable;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.CommitVersions;
import com.cinchapi.concourse.server.storage.WriteStream;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.cache.BloomFilters;
import com.cinchapi.concourse.server.storage.db.CorpusRevision;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.IndexRevision;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.TableRevision;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.lib.offheap.memory.OffHeapMemory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * A {@link Segment Segments} are where data in the {@link Database} is
 * physically stored.
 * <p>
 * When the {@link Database} {@link Database#accept(Write) accepts} a
 * {@link Write} it is {@link Segment#acquire(Write, AwaitableExecutorService)
 * transferred} to a {@link Segment} which uses several {@link Chunk
 * Chunks} internally to maintain optimized views of the data for different
 * operations.
 * </p>
 * <p>
 * {@link Segment Segments} and
 * {@link com.cinchapi.concourse.server.storage.temp.Buffer.Page Buffer pages}
 * work in tandem such that the data in a {@link Segment} initially corresponds
 * exactly to the from a
 * {@link com.cinchapi.concourse.server.storage.temp.Buffer.Page
 * Page} (<em>{@link Segment Segments} may later be merged or split to optimize
 * read performance</em>). When all the {@link Write Writes} from a
 * {@link com.cinchapi.concourse.server.storage.temp.Buffer.Page
 * Page} have been
 * {@link #acquire(Write, AwaitableExecutorService) transferred}, the
 * corresponding {@link Segment} is durably
 * {@link #sync(AwaitableExecutorService) synced} and
 * the {@link Buffer#Page Page} is
 * {@link com.cinchapi.concourse.server.storage.temp.Buffer.Page#delete()
 * deleted}
 * </p>
 * <p>
 * Prior to 0.2, Concourse stored each logical Record in its own file, which had
 * the advantage of simplified deserialization (e.g. we only needed to locate
 * one file and read all of its content). The down side to that approach was
 * that a single record couldn't be deserialized if it was larger than the
 * amount of available memory. Storing data in {@link Segment Segments}, helps
 * to solve that problem, because larger Records are broken up and we can do
 * more granular seeking to reduce the amount of data that must come into memory
 * (i.e. we can limit data reading by timestamp). {@link Segment Segments} also
 * make it much easier to nuke old data without reading anything. And since each
 * {@link Segment} relies on multiple {@link BloomFilter BloomFilters} and
 * {@link Manifest Manifests} we make best efforts to only look at files when
 * necessary.
 * </p>
 * <p>
 * Prior to 0.11, Concourse stored data for each {@link Segment} across at least
 * 12 different files using the {@link Block} framework and its associated
 * constructs. {@link Segment Segments} are now streamlined so that the data for
 * each one is stored in a single file and {@link Chunk Chunks} are used to
 * ensure that different read operations can leverage optimized views of the
 * data.
 * </p>
 *
 * @author Jeff Nelson
 */
/*
 * NOTE: Segment thread-safety is guaranteed by a #readLock and #writeLock.
 * The Segment is responsible for asynchronously writing data to each Chunk, so
 * it handles write locking here. On the other hand, it defers all reads to each
 * Chunk individually, so Chunks grab the Segment's #readLock for each read
 * operation.
 */
@Immutable
@ThreadSafe
public final class Segment extends TransferableByteSequence implements
        Itemizable,
        WriteStream {

    /**
     * Create a new {@link Segment}.
     * 
     * @return the {@link Segment}
     */
    public static Segment create() {
        return create(DEFAULT_EXPECTED_INSERTIONS);
    }

    /**
     * Create a new {@link Segment}.
     * 
     * @param expectedInsertions
     * @return the {@link Segment}
     */
    public static Segment create(int expectedInsertions) {
        return new Segment(expectedInsertions);
    }

    /**
     * Return a {@link Segment} where each of its {@link Chunk Chunks} is
     * {@link Chunk#shift(OffHeapMemory) shifted} to {@link OffHeapMemory}.
     * 
     * @param expectedInsertions
     * @return the {@link Segment}
     */
    public static Segment createOffHeap(int expectedInsertions) {
        Segment segment = create(expectedInsertions);
        long capacity = expectedInsertions * Write.MINIMUM_SIZE * 3;
        for (Chunk<? extends Byteable, ? extends Byteable, ? extends Byteable> chunk : Array
                .containing(segment.table, segment.index, segment.corpus)) {
            // NOTE: Using DirectMemory because UnsafeMemory causes a segfault
            // on CircleCI, so it isn't reliable yet. In the future, the kind of
            // OffHeapMemory to use may be configurable based on tuning.
            OffHeapMemory memory = OffHeapMemory.allocateDirect(capacity);
            chunk.shift(memory);
        }
        return segment;
    }

    /**
     * Load an existing {@link Segment} whose data is stored in {@code file}.
     * 
     * @param file
     * @return the loaded {@link Segment}
     * @throws SegmentLoadingException
     */
    public static Segment load(Path file) throws SegmentLoadingException {
        return new Segment(file);
    }

    /**
     * A {@link Comparator} that sorts {@link Segment Segments} chronologically
     * based on the timestamp range covered by this {@link Revision revisions}
     * in the {@link Segment}.
     * <p>
     * Each {@link Segment} is intended to cover a discrete range of time. If
     * two {@link Segment}'s have overlapping time ranges, it means that they
     * were either merged or split (e.g. optimization) and the {@link #syncTs}
     * is used to disambiguate.
     * </p>
     */
    public static Comparator<Segment> TEMPORAL_COMPARATOR = (o1, o2) -> {
        if(o1.maxTs < o2.minTs) {
            return -1;
        }
        else if(o1.minTs > o2.maxTs) {
            return 1;
        }
        else if(!o1.isMutable() && !o2.isMutable()) {
            // The immutable Segments have overlapping timestamps, so sort
            // based on the syncTs so that the newer Segment is "greater"
            return Long.compare(o1.syncTs, o2.syncTs);
        }
        else if(o1.isMutable()) {
            return 1;
        }
        else { // o2.mutable == true
            return -1;
        }
    };

    /**
     * Multiplied times the number of expected insertions in an attempt to size
     * the {@link Chunk#filter() bloom filter} of each {@link Chunk} correctly.
     */
    private static final int BLOOM_FILTER_ENTRIES_PER_INSERTION_MULTIPLE = 3;

    /**
     * The expected number of Segment insertions. This number is used to size
     * the Segment's internal data structures. This value should be large enough
     * to reflect the fact that, for each revision, we make 3 inserts into the
     * bloom filter, but no larger than necessary since we must keep all bloom
     * filters in memory.
     */
    private static final int DEFAULT_EXPECTED_INSERTIONS = GlobalState.BUFFER_PAGE_SIZE
            / Write.MINIMUM_SIZE;

    /**
     * The expected bytes at the beginning of a {@link Segment} file to properly
     * identify it.
     */
    private static byte[] FILE_SIGNATURE = ByteBuffers
            .fromUtf8String("Cinchapi Inc.").array();

    /**
     * The number of bytes that are dedicated to storing metadata about this
     * {@link Segment} when it is {@link #fsync(Path) synced}.
     */
    // @formatter:off
    private static int METADATA_LENGTH =
            FILE_SIGNATURE.length
            + 1 // version
            + 8 // count
            + 8 // minTs
            + 8 // maxTs
            + 8 // syncTs
            + 8 // reserved for future use
            + 8 // reserved for future use
            + 8 // reserved for future use
            + 8 // reserved for future use
            + 8 // table.filter().size()
            + 8 // index.filter().size()
            + 8 // corpus.fiter().size()
            + 8 // table.manifest.size()
            + 8 // index.manifest.size()
            + 8 // corpus.manifest.size()
            + 8 // table.size()
            + 8 // index.size()
            + 8 // corpus.size()
            ;
    // @formatter:on

    /**
     * Multiplied times the number of expected insertions in an attempt to size
     * the {@link #objects} pool correctly.
     * <p>
     * For a given Write(key, value, record) we have the following insertions:
     * <ol>
     * <li>key (table as K)</li>
     * <li>key (index as L)</li>
     * <li>key (corpus as L)</li>
     * <li>value (table as V)</li>
     * <li>value (index as K)</li>
     * <li>record (table as L)</li>
     * <li>record (index as V)</li>
     * <li>$position (corpus as V)</li>
     * </ol>
     * </p>
     */
    private static final int OBJECTS_POOL_ENTRIES_PER_INSERTION_MULTIPLE = 8
            + (GlobalState.MAX_SEARCH_SUBSTRING_LENGTH > 0
                    ? GlobalState.MAX_SEARCH_SUBSTRING_LENGTH
                    : 100);

    /**
     * The current schema version.
     */
    private static byte SCHEMA_VERSION = 1;

    /**
     * The number of {@link Revision Revisions} that has been
     * {@link #acquire(Write, AwaitableExecutorService) transferred}.
     */
    protected long count = -1;

    /**
     * The largest timestamp associated with a {@link Write} that has been
     * {@link #acquire(Write, AwaitableExecutorService) transferred} to this
     * {@link Segment}.
     */
    protected long maxTs;

    /**
     * The smallest timestamp associated with a {@link Write} that has been
     * {@link #acquire(Write, AwaitableExecutorService) transferred} to this
     * {@link Segment}.
     */
    protected long minTs;

    /**
     * Read lock.
     */
    protected ReadLock readLock = read;

    /**
     * The timestamp when this {@link Segment} was {@link #fsync(Path) synced}
     * to disk.
     */
    protected long syncTs;

    /**
     * Write lock.
     */
    protected WriteLock writeLock = write;

    /**
     * The {@link CorpusChunk} that contains a searchable view of the data.
     */
    private final CorpusChunk corpus;

    /**
     * The {@link IndexChunk} that contains an inverted view of the data.
     */
    private final IndexChunk index;

    /**
     * A collection of all the known objects that have been acquired by
     * this {@link Segment} from {@link Write} components and are added to
     * {@link Chunk Chunks} as either a locator, key or value.
     * 
     * <p>
     * This collection is {@link #objects() available} so that each
     * {@link Chunk} can consult it in {@link Chunk#deduplicate(Object)} prior
     * to {@link Chunk#makeRevision(Byteable, Byteable, Byteable, long, Action)
     * making the revisions} that are stored in an effort to avoid unnecessary
     * memory duplication within the {@link Chunk} itself and across other
     * {@link Chunks} in the {@link Segment}.
     * <p>
     * This collection is only populated while the {@link Segment} is
     * {@link #isMutable() mutable} and is nullified when the memory is
     * {@link #free() freed} as part of the {@link #transfer(Path) transfer}
     * process.
     * </p>
     */
    private Map<Byteable, Byteable> objects;

    /**
     * The {@link TableChunk} that contains the logical view of the data.
     */
    private final TableChunk table;

    /**
     * The schema version at which this {@link Segment} was written.
     */
    private byte version;

    /**
     * Construct a new instance.
     */
    private Segment(int expectedInsertions) {
        super();
        this.objects = new ConcurrentHashMap<>(expectedInsertions
                * OBJECTS_POOL_ENTRIES_PER_INSERTION_MULTIPLE);
        this.maxTs = Long.MIN_VALUE;
        this.minTs = Long.MAX_VALUE;
        this.syncTs = 0;
        this.table = TableChunk.create(this,
                BloomFilter.create(BLOOM_FILTER_ENTRIES_PER_INSERTION_MULTIPLE
                        * expectedInsertions));
        this.index = IndexChunk.create(this,
                BloomFilter.create(BLOOM_FILTER_ENTRIES_PER_INSERTION_MULTIPLE
                        * expectedInsertions));
        this.corpus = CorpusChunk.create(this,
                BloomFilter
                        .create(10 * BLOOM_FILTER_ENTRIES_PER_INSERTION_MULTIPLE
                                * expectedInsertions));
        this.version = SCHEMA_VERSION;
    }

    /**
     * Load an existing instance.
     * 
     * @param file
     * @throws SegmentLoadingException
     */
    private Segment(Path file) throws SegmentLoadingException {
        super(file);
        this.objects = null;
        try {
            ByteBuffer metadata = ByteBuffer.allocate(METADATA_LENGTH);
            channel().read(metadata);
            metadata.flip();
            byte[] signature = new byte[FILE_SIGNATURE.length];
            metadata.get(signature);
            if(Arrays.equals(signature, FILE_SIGNATURE)) {
                this.version = metadata.get();
                this.count = metadata.getLong();
                this.minTs = metadata.getLong();
                this.maxTs = metadata.getLong();
                this.syncTs = metadata.getLong();
                metadata.getLong(); // reserved
                metadata.getLong(); // reserved
                metadata.getLong(); // reserved
                metadata.getLong(); // reserved
                long tableFilterLength = metadata.getLong();
                long indexFilterLength = metadata.getLong();
                long corpusFilterLength = metadata.getLong();
                long tableManifestLength = metadata.getLong();
                long indexManifestLength = metadata.getLong();
                long corpusManifestLength = metadata.getLong();
                long tableLength = metadata.getLong();
                long indexLength = metadata.getLong();
                long corpusLength = metadata.getLong();
                long position = METADATA_LENGTH;

                ByteBuffer filterBytes = channel().map(MapMode.READ_ONLY,
                        position, tableFilterLength + indexFilterLength
                                + corpusFilterLength);
                position += filterBytes.capacity();

                // Table BloomFilter
                BloomFilter tableFilter = BloomFilter.load(ByteBuffers
                        .slice(filterBytes, 0, (int) tableFilterLength));

                // Index BloomFilter
                BloomFilter indexFilter = BloomFilter.load(
                        ByteBuffers.slice(filterBytes, (int) tableFilterLength,
                                (int) indexFilterLength));

                // Corpus BloomFilter
                BloomFilter corpusFilter = BloomFilter
                        .load(ByteBuffers.slice(filterBytes,
                                (int) (tableFilterLength + indexFilterLength),
                                (int) corpusFilterLength));

                // Table Manifest
                Manifest tableManifest = Manifest.load(this, position,
                        tableManifestLength);
                position += tableManifestLength;

                // Index Manifest
                Manifest indexManifest = Manifest.load(this, position,
                        indexManifestLength);
                position += indexManifestLength;

                // Corpus Manifest
                Manifest corpusManifest = Manifest.load(this, position,
                        corpusManifestLength);
                position += corpusManifestLength;

                // Table
                this.table = TableChunk.load(this, position, tableLength,
                        tableFilter, tableManifest);
                position += tableLength;

                // Index
                this.index = IndexChunk.load(this, position, indexLength,
                        indexFilter, indexManifest);
                position += indexLength;

                // Corpus
                this.corpus = CorpusChunk.load(this, position, corpusLength,
                        corpusFilter, corpusManifest);
            }
            else {
                throw new SegmentLoadingException(
                        file + " is not a valid Segment file");
            }
        }
        catch (Exception e) {
            throw new SegmentLoadingException(e.getMessage(), e);
        }
    }

    /**
     * Append the {@code write} to this {@link Segment}.
     * <p>
     * This method is only suitable for testing.
     * </p>
     * 
     * @param write
     * @return a {@link Receipt} that contains the {@link Revision Revisions}
     *         that were created as a consequence of the transfer
     */
    public Receipt acquire(Write write) {
        try {
            return acquire(write, new AwaitableExecutorService(
                    MoreExecutors.newDirectExecutorService()));
        }
        catch (InterruptedException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Append the {@code write} to this {@link Segment} using the
     * {@code executor} to asynchronously write to all the contained
     * {@link Block blocks}.
     * 
     * @param write
     * @param executor
     * @return a {@link Receipt} that contains the {@link Revision Revisions}
     *         that were created as a consequence of the transfer
     * @throws InterruptedException
     */
    public Receipt acquire(Write write, AwaitableExecutorService executor)
            throws InterruptedException {
        Preconditions.checkState(isMutable(),
                "Cannot transfer Writes to an immutable Segment");
        writeLock.lock();
        try {
            // @formatter:off
            Identifier record = write.getRecord();
            Text key          = write.getKey();
            Value value       = write.getValue();
            long version      = write.getVersion();
            Action type       = write.getType();            
            Receipt.Builder receipt = Receipt.builder();
            Runnable[] tasks = Array.containing(() -> {
                TableArtifact artifact = table.insert(
                        record, key, value, version, type);
                receipt.itemize(artifact);
            }, () -> {
                IndexArtifact artifact = index.insert(
                        key, value, record, version, type);
                receipt.itemize(artifact);
            }, () -> {
                Collection<CorpusArtifact> artifacts = corpus.insert(
                        key, value, record, version, type);
                receipt.itemize(artifacts);
            });
            // @formatter:on
            executor.await((task, error) -> Logger.error(
                    "Unexpected error when trying to transfer the following Write to the Database: {}",
                    write, error), tasks);
            // CON-587: Set the min/max version of this Segment because it
            // cannot be assumed that revisions are inserted in monotonically
            // increasing order of version.
            maxTs = Math.max(write.getVersion(), maxTs);
            minTs = Math.min(write.getVersion(), minTs);
            return receipt.build();
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public void append(Write write) {
        acquire(write);
    }

    /**
     * Return this {@link Segment Segment's} {@link CorpusChunk}, if it exists.
     * 
     * @return the {@link CorpusChunk} or {@code null} it it does not exist
     */
    public CorpusChunk corpus() {
        return corpus;
    }

    @Override
    public long count() {
        return isMutable() ? index.count() : count;
    }

    /**
     * Permanently delete an immutable {@link Segment} by clearing its contents
     * from both memory and disk.
     * <p>
     * This is an irreversible and potentially catastrophic operation. It should
     * only be used instances when the caller can be certain that the
     * {@link Segment} is no longer needed.
     * </p>
     * <p>
     * A deleted {@link Segment} should not be subsequently used. Operating on a
     * deleted {@link Segment} has undefined behaviour.
     * </p>
     */
    public final void delete() {
        Preconditions.checkState(!isMutable(),
                "A mutable segment cannot be deleted");
        Logger.warn("Permanently deleting Segment {}", this);
        Reflection.set("table", null, this); /* (authorized) */
        Reflection.set("index", null, this); /* (authorized) */
        Reflection.set("corpus", null, this); /* (authorized) */
        free();
        FileSystem.deleteFile(file().toString());
    }

    /**
     * Return a {@link Set} (not necessarily sorted) of all the
     * {@link Write#hash() hashes} represented by the data
     * {@link #acquire(Write) acquired} by this {@link Segment}.
     * 
     * @return the contained data {@link Write#hash() hashes}
     */
    public Set<HashCode> hashes() {
        return writes().map(Write::hash)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Return the {@code id} of this {@link Segment}.
     * 
     * @return the segment id
     */
    public String id() {
        return !isMutable() ? file().getFileName().toString()
                : "MutableSegment-" + System.identityHashCode(this);
    }

    /**
     * Return this {@link Segment Segment's} {@link IndexChunk}.
     * 
     * @return the {@link IndexChunk}
     */
    public IndexChunk index() {
        return index;
    }

    /**
     * Return {@code true} if the Set of {@link #hashes()} in this
     * {@link Segment} intersect with those of the {@code other} one.
     * 
     * @param other
     * @return {@code true} if the {@link Segment Segments} intersect
     */
    public boolean intersects(Segment other) {
        Range<Long> r1 = count > 0 ? Range.closed(minTs, maxTs)
                : Range.greaterThan(CommitVersions.next());
        Range<Long> r2 = other.count > 0
                ? Range.closed(other.minTs, other.maxTs)
                : Range.greaterThan(CommitVersions.next());
        return r1.isConnected(r2)
                && !Sets.intersection(hashes(), other.hashes()).isEmpty();
    }

    @Override
    public long length() {
        boolean mutable = isMutable();
        Locks.lockIfCondition(read, mutable);
        try {
            if(mutable) {
                // @formatter:off
                long size = 
                    METADATA_LENGTH 
                    + table.filter().size()
                    + index.filter().size() 
                    + corpus.filter().size()
                    + table.manifest().length() 
                    + index.manifest().length() 
                    + corpus.manifest().length()  
                    + table.length() 
                    + index.length()
                    + corpus.length()
                ;
                // @formatter:on
                return size;
            }
            else {
                return FileSystem.getFileSize(file().toString());
            }
        }
        finally {
            Locks.unlockIfCondition(read, mutable);
        }
    }

    /**
     * Reindex this {@link Segment} by replaying the {@link #acquire(Write)} of
     * its {@link #writes()} to a {@link #Segment() new} {@link Segment}.
     * 
     * @return the reindexed {@link Segment}
     */
    public Segment reindex() {
        Segment segment = new Segment((int) count());
        writes().forEach(segment::acquire);
        return segment;
    }

    /**
     * Return an <strong>estimated</strong> Jaccard Index
     * (https://en.wikipedia.org/wiki/Jaccard_index); a number between 0 and 1
     * that indicates how similar {@code this} {@link Segment} is to the
     * {@code other} one based on the {@link Writes} that have been
     * {@link #acquire(Write, AwaitableExecutorService) transferred}.
     * <p>
     * This method doesn't read the stored {@link Revision revisions}, but
     * instead uses the {@link BloomFilter BloomFilters} of some of its
     * {@link Chunk Chunks} to efficiently estimate the similarity between the
     * data stored in each {@link Segment}.
     * </p>
     * 
     * @param other
     * @return a number between 0 and 1 that gives an estimate of how similar
     *         the two segments are
     */
    public double similarityWith(Segment other) {
        try {
            return Math.max(
                    BloomFilters.estimateSimilarity(table.filter(),
                            other.table.filter()),
                    BloomFilters.estimateSimilarity(index.filter(),
                            other.index.filter()));
        }
        catch (IllegalArgumentException e) {
            return 0.0;
        }
    }

    /**
     * Return this {@link Segment Segment's} {@link TableChunk}.
     * 
     * @return the {@link TableChunk}
     */
    public TableChunk table() {
        return table;
    }

    @Override
    public String toString() {
        return id();
    }

    /**
     * Return a {@link Stream} containing all the {@link Write Writes} that were
     * {@link #acquire(Write, AwaitableExecutorService) acquired} by this
     * {@link Segment}.
     * 
     * @return the transferred {@link Write Writes}
     */
    @Override
    public Stream<Write> writes() {
        return StreamSupport.stream(table.spliterator(), false)
                .map(revision -> Reflection.newInstance(Write.class,
                        revision.getType(), revision.getKey(),
                        revision.getValue(), revision.getLocator(),
                        revision.getVersion()));
    }

    @Override
    protected void flush(ByteSink sink) {
        this.syncTs = Time.now();
        this.count = index.count();
        sink.put(FILE_SIGNATURE);
        sink.put(version);
        sink.putLong(count);
        sink.putLong(minTs);
        sink.putLong(maxTs);
        sink.putLong(syncTs);
        sink.putLong(0);
        sink.putLong(0);
        sink.putLong(0);
        sink.putLong(0);
        sink.putLong(table.filter().size());
        sink.putLong(index.filter().size());
        sink.putLong(corpus.filter().size());
        sink.putLong(table.manifest().length());
        sink.putLong(index.manifest().length());
        sink.putLong(corpus.manifest().length());
        sink.putLong(table.length());
        sink.putLong(index.length());
        sink.putLong(corpus.length());

        // @formatter:off
        for (Byteable byteable :  Array.containing(
                table.filter(), 
                index.filter(), 
                corpus.filter()
        )) {
            byteable.copyTo(sink);
        }
        for (TransferableByteSequence sequence : Array.containing(
                table.manifest(), 
                index.manifest(), 
                corpus.manifest(),
                table,                
                index,
                corpus
        )) {
            sequence.transfer(sink);
        }
        // @formatter:on
    }

    @Override
    protected void free() {
        Logger.debug("Freeing memory used by {}", this);
        this.objects = null;
    }

    @Nullable
    protected final Map<Byteable, Byteable> objects() {
        return objects;
    }

    /**
     * A {@link Receipt} is acknowledges the successful
     * {@link Segment#acquire(Write, AwaitableExecutorService) transfer} of a
     * {@link Write} to a {@link Segment} and includes the {@link Revision
     * revisions} that were created in the Segment's storage {@link Block
     * Blocks}.
     *
     * @author Jeff Nelson
     */
    @Immutable
    public static class Receipt {

        /**
         * Return an incremental {@link Receipt} {@link Builder}.
         * 
         * @return the {@link Builder}
         */
        static Builder builder() {
            return new Builder();
        }

        private final Collection<CorpusArtifact> corpus;
        private final IndexArtifact index;
        private final TableArtifact table;

        /**
         * Construct a new instance.
         * 
         * @param table
         * @param index
         * @param corpus
         */
        Receipt(TableArtifact table, IndexArtifact index,
                Collection<CorpusArtifact> corpus) {
            this.table = table;
            this.index = index;
            this.corpus = corpus;
        }

        /**
         * Return the {@link CorpusRevision CorpusRevisions} included with this
         * {@link Receipt}.
         * 
         * @return the {@link CorpusRevision CorpusRevisions}
         */
        public Collection<CorpusArtifact> corpus() {
            return corpus;
        }

        /**
         * Return the {@link IndexRevision} included with this
         * {@link Receipt}.
         * 
         * @return the {@link IndexRevision}
         */
        public IndexArtifact index() {
            return index;
        }

        /**
         * Return the {@link TableRevision} included with this
         * {@link Receipt}.
         * 
         * @return the {@link TableRevision}
         */
        public TableArtifact table() {
            return table;
        }

        /**
         * {@link Receipt} builder.
         */
        static class Builder {

            Collection<CorpusArtifact> corpus;
            IndexArtifact index;
            TableArtifact table;

            /**
             * Build and return the {@link Receipt}.
             * 
             * @return the built {@link Receipt}
             */
            Receipt build() {
                return new Receipt(table, index, corpus);
            }

            /**
             * Add the {@code artifacts} to the {@link Receipt}.
             * 
             * @param artifacts
             * @return this
             */
            Builder itemize(Collection<CorpusArtifact> artifacts) {
                corpus = artifacts;
                return this;
            }

            /**
             * Add the {@code artifact} to the {@link Receipt}.
             * 
             * @param artifact
             * @return this
             */
            Builder itemize(IndexArtifact artifact) {
                index = artifact;
                return this;
            }

            /**
             * Add the {@code artifact} to the {@link Receipt}.
             * 
             * @param artifact
             * @return this
             */
            Builder itemize(TableArtifact artifact) {
                table = artifact;
                return this;
            }
        }

    }
}
