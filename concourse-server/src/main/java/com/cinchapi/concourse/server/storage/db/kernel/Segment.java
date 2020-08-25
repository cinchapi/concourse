/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
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
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.Freezable;
import com.cinchapi.concourse.server.io.Itemizable;
import com.cinchapi.concourse.server.io.Syncable;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.cache.BloomFilters;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.IndexRevision;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.TableRevision;
import com.cinchapi.concourse.server.storage.db.kernel.Chunk.Folio;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * A {@link Segment Segments} are where data in the {@link Database} is
 * physically stored.
 * <p>
 * When the {@link Database} {@link Database#accept(Write) accepts} a
 * {@link Write} it is {@link Segment#transfer(Write, AwaitableExecutorService)
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
 * {@link #transfer(Write, AwaitableExecutorService) transferred}, the
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
 * The Segment is responsibility for asynchronously writing data to each
 * Chunk, so it handles write locking here. On the other hand, it defers all
 * reads to each Chunk individually, so Chunks grab the Segment's #readLock
 * for each read operation.
 */
@Immutable
@ThreadSafe
public final class Segment implements Itemizable, Syncable {

    /**
     * Create a new {@link Segment}.
     * 
     * @return the {@link Segment}
     */
    public static Segment create() {
        return create(EXPECTED_INSERTIONS);
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
        else if(!o1.mutable && !o2.mutable) {
            // The immutable Segments have overlapping timestamps, so sort
            // based on the syncTs so that the newer Segment is "greater"
            return Longs.compare(o1.syncTs, o2.syncTs);
        }
        else if(o1.mutable) {
            return 1;
        }
        else { // o2.mutable == true
            return -1;
        }
    };

    /**
     * The expected number of Segment insertions. This number is used to size
     * the Segment's internal data structures. This value should be large enough
     * to reflect the fact that, for each revision, we make 3 inserts into the
     * bloom filter, but no larger than necessary since we must keep all bloom
     * filters in memory.
     */
    private static final int EXPECTED_INSERTIONS = GlobalState.BUFFER_PAGE_SIZE;

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
     * The current schema version.
     */
    private static byte SCHEMA_VERSION = 1;

    /**
     * The upper limit (in terms of number of bytes) when there is performance
     * stagnation or degradation from
     * {@link #mmap(Path, int, Folio, Folio, Folio) using a memory-mapped file}
     * to {@link #fsync(Path)} and a
     * {@link #write(Path, int, Folio, Folio, Folio) file channel} should be
     * used instead.
     */
    private static int MMAP_WRITE_UPPER_LIMIT = 419430400;

    /**
     * The largest timestamp associated with a {@link Write} that has been
     * {@link #transfer(Write, AwaitableExecutorService) transferred} to this
     * {@link Segment}.
     */
    protected long maxTs;

    /**
     * The smallest timestamp associated with a {@link Write} that has been
     * {@link #transfer(Write, AwaitableExecutorService) transferred} to this
     * {@link Segment}.
     */
    protected long minTs;

    /**
     * The timestamp when this {@link Segment} was {@link #fsync(Path) synced}
     * to disk.
     */
    protected long syncTs;

    /**
     * The number of {@link Revision Revisions} that has been
     * {@link #transfer(Write, AwaitableExecutorService) transferred}.
     */
    protected long count = -1;

    /**
     * The {@link CorpusChunk} that contains a searchable view of the data.
     */
    private final CorpusChunk corpus;

    /**
     * The file where this {@link Segment Segment's} data is stored when
     * {@link #fsync(Path) synced}.
     */
    @Nullable
    private Path file;

    /**
     * The {@link IndexChunk} that contains an inverted view of the data.
     */
    private final IndexChunk index;

    /**
     * A flag that indicates whether this {@link Segment} is mutable.
     */
    private boolean mutable;

    /**
     * The {@link TableChunk} that contains the logical view of the data.
     */
    private final TableChunk table;

    /**
     * The schema version at which this {@link Segment} was written.
     */
    private byte version;

    /**
     * Provider for {@link #readLock} and {@link #writeLock} locks.
     */
    private final ReentrantReadWriteLock master = new ReentrantReadWriteLock();

    /**
     * Read lock.
     */
    protected final ReadLock readLock = master.readLock();

    /**
     * Write lock.
     */
    private final WriteLock writeLock = master.writeLock();

    /**
     * Construct a new instance.
     */
    private Segment(int expectedInsertions) {
        this.mutable = true;
        this.file = null;
        this.maxTs = Long.MIN_VALUE;
        this.minTs = Long.MAX_VALUE;
        this.syncTs = 0;
        this.table = TableChunk.create(this,
                BloomFilter.create(expectedInsertions));
        this.index = IndexChunk.create(this,
                BloomFilter.create(expectedInsertions));
        this.corpus = CorpusChunk.create(this,
                BloomFilter.create(expectedInsertions));
        this.version = SCHEMA_VERSION;
    }

    /**
     * Load an existing instance.
     * 
     * @param file
     * @throws SegmentLoadingException
     */
    private Segment(Path file) throws SegmentLoadingException {
        this.mutable = false;
        this.file = file;
        FileChannel channel = FileSystem.getFileChannel(file);
        try {
            ByteBuffer metadata = ByteBuffer.allocate(METADATA_LENGTH);
            channel.read(metadata);
            metadata.flip();
            byte[] signature = new byte[FILE_SIGNATURE.length];
            metadata.get(signature);
            if(Arrays.equals(signature, FILE_SIGNATURE)) {
                long position = 0;
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
                position += METADATA_LENGTH;

                ByteBuffer filterBytes = channel.map(MapMode.READ_ONLY,
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
                BloomFilter corpusFilter = BloomFilter.load(
                        ByteBuffers.slice(filterBytes, (int) indexFilterLength,
                                (int) corpusFilterLength));

                // Table Manifest
                Manifest tableManifest = Manifest.load(file, position,
                        tableManifestLength);
                position += tableManifestLength;

                // Index Manifest
                Manifest indexManifest = Manifest.load(file, position,
                        indexManifestLength);
                position += indexManifestLength;

                // Corpus Manifest
                Manifest corpusManifest = Manifest.load(file, position,
                        corpusManifestLength);
                position += corpusManifestLength;

                // Table
                this.table = TableChunk.load(this, file, position, tableLength,
                        tableFilter, tableManifest);
                position += tableLength;

                // Index
                this.index = IndexChunk.load(this, file, position, indexLength,
                        indexFilter, indexManifest);
                position += indexLength;

                // Corpus
                this.corpus = CorpusChunk.load(this, file, position,
                        corpusLength, corpusFilter, corpusManifest);
            }
            else {
                throw new SegmentLoadingException(
                        file + " is not a valid Segment file");
            }
        }
        catch (IOException e) {
            throw new SegmentLoadingException(e.getMessage(), e);
        }
        finally {
            FileSystem.closeFileChannel(channel);
        }
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
        return mutable ? index.count() : count;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof Segment) {
            Segment seg = (Segment) other;
            Range<Long> r1 = Range.closed(minTs, maxTs);
            Range<Long> r2 = Range.closed(seg.minTs, seg.maxTs);
            return r1.isConnected(r2);
        }
        else {
            return false;
        }
    }

    @Override
    public void fsync(Path file) {
        Preconditions.checkState(mutable, "Cannot fsync an immutable Segment");
        readLock.lock();
        try {
            this.mutable = false;
            this.syncTs = Time.now();
            this.count = index.count();
            Folio tableFolio = table.serialize();
            Folio indexFolio = index.serialize();
            Folio corpusFolio = corpus.serialize();
            // @formatter:off
            int size = 
                METADATA_LENGTH 
                + table.filter().size()
                + index.filter().size() 
                + corpus.filter().size()
                + tableFolio.manifest().size() 
                + indexFolio.manifest().size()
                + corpusFolio.manifest().size() 
                + table.size() 
                + index.size()
                + corpus.size()
            ;
            // @formatter:on
            if(size <= MMAP_WRITE_UPPER_LIMIT) {
                mmap(file, size, tableFolio, indexFolio, corpusFolio);
            }
            else {
                write(file, size, tableFolio, indexFolio, corpusFolio);
            }
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(minTs, maxTs);
    }

    /**
     * Return the {@code id} of this {@link Segment}.
     * 
     * @return the segment id
     */
    public String id() {
        return file != null ? file.getFileName().toString() : label();
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
     * Return an ephemeral label for this {@link Segment}. This label is not
     * consistent and should not be relied upon for identification.
     * 
     * @return the label
     */
    public String label() {
        return "Segment-" + System.identityHashCode(this);
    }

    /**
     * Return {@code true} if this {@link Segment} can
     * {@link #transfer(Write, AwaitableExecutorService) transfer} data.
     * 
     * @return a boolean indicating if this {@link Segment} is mutable
     */
    public boolean isMutable() {
        return mutable;
    }

    /**
     * Reindex this {@link Segment} by replaying the {@link #transfer(Write)} of
     * its {@link #writes()} to a {@link #Segment() new} {@link Segment}.
     * 
     * @return the reindexed {@link Segment}
     */
    public Segment reindex() {
        Segment segment = new Segment((int) count());
        writes().forEach(segment::transfer);
        return segment;
    }

    /**
     * Return an <strong>estimated</strong> Jaccard Index
     * (https://en.wikipedia.org/wiki/Jaccard_index); a number between 0 and 1
     * that indicates how similar {@code this} {@link Segment} is to the
     * {@code other} one based on the {@link Writes} that have been
     * {@link #transfer(Write, AwaitableExecutorService) transferred}.
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
     * Return the size of this {@link Segment} if it is {@link #isMutable()
     * immutable}.
     * 
     * @return the {@link Segment} size
     */
    public long size() {
        Preconditions.checkState(!isMutable());
        return FileSystem.getFileSize(file.toString());
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
     * Transfer the {@code write} to this {@link Segment}.
     * <p>
     * This method is only suitable for testing.
     * </p>
     * 
     * @param write
     * @return a {@link Receipt} that contains the {@link Revision Revisions}
     *         that were created as a consequence of the transfer
     */
    public Receipt transfer(Write write) {
        try {
            return transfer(write, new AwaitableExecutorService(
                    MoreExecutors.newDirectExecutorService()));
        }
        catch (InterruptedException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Transfer the {@code write} to this {@link Segment} using the
     * {@code executor} to asynchronously write to all the contained
     * {@link Block blocks}.
     * 
     * @param write
     * @param executor
     * @return a {@link Receipt} that contains the {@link Revision Revisions}
     *         that were created as a consequence of the transfer
     * @throws InterruptedException
     */
    public Receipt transfer(Write write, AwaitableExecutorService executor)
            throws InterruptedException {
        Preconditions.checkState(mutable,
                "Cannot transfer Writes to an immutable Segment");
        writeLock.lock();
        try {
            Receipt.Builder receipt = Receipt.builder();
            Runnable[] tasks = Array.containing(() -> {
                TableRevision revision = table.insert(write.getRecord(),
                        write.getKey(), write.getValue(), write.getVersion(),
                        write.getType());
                receipt.itemize(revision);
            }, () -> {
                IndexRevision revision = index.insert(write.getKey(),
                        write.getValue(), write.getRecord(), write.getVersion(),
                        write.getType());
                receipt.itemize(revision);
            }, () -> {
                corpus.insert(write.getKey(), write.getValue(),
                        write.getRecord(), write.getVersion(), write.getType());
                // NOTE: We do not itemize a CorpusRevision within the receipt
                // because the database does not cache CorpusRecords since they
                // have the potential to be VERY large. Holding references to
                // them in a database's cache would prevent them from being
                // garbage collected resulting in more OOMs.
            });
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

    /**
     * Return a {@link Stream} containing all the {@link Write Writes} that were
     * {@link #transfer(Write, AwaitableExecutorService) transferred} to this
     * {@link Segment}.
     * 
     * @return the transferred {@link Write Writes}
     */
    public Stream<Write> writes() {
        return StreamSupport.stream(table.spliterator(), false)
                .map(revision -> Reflection.newInstance(Write.class,
                        revision.getType(), revision.getKey(),
                        revision.getValue(), revision.getLocator(),
                        revision.getVersion()));
    }

    /**
     * Implementation of {@link #fsync(Path)} using a memory mapped file.
     * 
     * @param file
     * @param size
     * @param tableFolio
     * @param indexFolio
     * @param corpusFolio
     */
    private void mmap(Path file, int size, Folio tableFolio, Folio indexFolio,
            Folio corpusFolio) {
        MappedByteBuffer bytes = FileSystem.map(file, MapMode.READ_WRITE, 0,
                size);
        bytes.put(FILE_SIGNATURE);
        bytes.put(version);
        bytes.putLong(count);
        bytes.putLong(minTs);
        bytes.putLong(maxTs);
        bytes.putLong(syncTs);
        bytes.putLong(0);
        bytes.putLong(0);
        bytes.putLong(0);
        bytes.putLong(0);
        bytes.putLong(table.filter().size());
        bytes.putLong(index.filter().size());
        bytes.putLong(corpus.filter().size());
        bytes.putLong(tableFolio.manifest().size());
        bytes.putLong(indexFolio.manifest().size());
        bytes.putLong(corpusFolio.manifest().size());
        bytes.putLong(table.size());
        bytes.putLong(index.size());
        bytes.putLong(corpus.size());

        // @formatter:off
        for (Byteable byteable :  Array.containing(
                table.filter(), // Table BloomFilter
                index.filter(), // Index BloomFilter
                corpus.filter() // Corpus BloomFilter
        )) {
            byteable.copyTo(bytes);
        }
        for (Manifest manifest : Array.containing(
                tableFolio.manifest(), // Table Manifest
                indexFolio.manifest(), // Index Manifest
                corpusFolio.manifest()  // Corpus Manifest
        )) {
            manifest.freeze(file, bytes.position());
            manifest.copyTo(bytes);
        }
        for (Part part : Array.containing(
                new Part(table, tableFolio.bytes()), // Table
                new Part(index, indexFolio.bytes()), // Index
                new Part(corpus, corpusFolio.bytes()) // Corpus
        )) {
            part.freeze(file, bytes.position());
            bytes.put(part.bytes());
        }
        // @formatter:on

        bytes.force();
        this.file = file;
    }

    /**
     * Implementation of {@link #fsync(Path)} using a {@link FileChannel}.
     * 
     * @param file
     * @param size
     * @param tableFolio
     * @param indexFolio
     * @param corpusFolio
     */
    private void write(Path file, int size, Folio tableFolio, Folio indexFolio,
            Folio corpusFolio) {
        FileChannel channel = FileSystem.getFileChannel(file);
        try {
            ByteBuffer metadata = ByteBuffer.allocate(METADATA_LENGTH);
            metadata.put(FILE_SIGNATURE);
            metadata.put(version);
            metadata.putLong(count);
            metadata.putLong(minTs);
            metadata.putLong(maxTs);
            metadata.putLong(syncTs);
            metadata.putLong(0);
            metadata.putLong(0);
            metadata.putLong(0);
            metadata.putLong(0);
            metadata.putLong(table.filter().size());
            metadata.putLong(index.filter().size());
            metadata.putLong(corpus.filter().size());
            metadata.putLong(tableFolio.manifest().size());
            metadata.putLong(indexFolio.manifest().size());
            metadata.putLong(corpusFolio.manifest().size());
            metadata.putLong(table.size());
            metadata.putLong(index.size());
            metadata.putLong(corpus.size());
            metadata.flip();
            while (metadata.hasRemaining()) {
                channel.write(metadata);
            }

            // @formatter:off
            for (ByteBuffer bytes :  Array.containing(
                    table.filter().getBytes(), // Table BloomFilter
                    index.filter().getBytes(), // Index BloomFilter
                    corpus.filter().getBytes() // Corpus BloomFilter
            )) {
                while (bytes.hasRemaining()) {
                    channel.write(bytes);
                }
            }
            for (Part part : Array.containing(
                    new Part(tableFolio.manifest(), tableFolio.manifest().getBytes()),   // Table Manifest
                    new Part(indexFolio.manifest(), indexFolio.manifest().getBytes()),   // Index Manifest
                    new Part(corpusFolio.manifest(), corpusFolio.manifest().getBytes()), // Corpus Manifest
                    new Part(table, tableFolio.bytes()),  // Table
                    new Part(index, indexFolio.bytes()),  // Index
                    new Part(corpus, corpusFolio.bytes()) // Corpus
            )) {
                part.freeze(file, channel.position());
                while (part.bytes().hasRemaining()) {
                    channel.write(part.bytes());
                }
            }
            // @formatter:on

            channel.force(true);
            this.file = file;
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            FileSystem.closeFileChannel(channel);
        }
    }

    /**
     * A {@link Receipt} is acknowledges the successful
     * {@link Segment#transfer(Write, AwaitableExecutorService) transfer} of a
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

        private final IndexRevision indexRevision;
        private final TableRevision tableRevision;

        /**
         * Construct a new instance.
         * 
         * @param primaryRevision
         * @param secondaryRevision
         */
        Receipt(TableRevision primaryRevision,
                IndexRevision secondaryRevision) {
            this.tableRevision = primaryRevision;
            this.indexRevision = secondaryRevision;
        }

        /**
         * Return the {@link IndexRevision} included with this
         * {@link Receipt}.
         * 
         * @return the {@link IndexRevision}
         */
        public IndexRevision indexRevision() {
            return indexRevision;
        }

        /**
         * Return the {@link TableRevision} included with this
         * {@link Receipt}.
         * 
         * @return the {@link TableRevision}
         */
        public TableRevision tableRevision() {
            return tableRevision;
        }

        /**
         * {@link Receipt} builder.
         */
        static class Builder {

            IndexRevision indexRevision;
            TableRevision tableRevision;

            /**
             * Build and return the {@link Receipt}.
             * 
             * @return the built {@link Receipt}
             */
            Receipt build() {
                return new Receipt(tableRevision, indexRevision);
            }

            /**
             * Add the {@code revision} to the {@link Receipt}.
             * 
             * @param revision
             * @return this
             */
            Builder itemize(TableRevision revision) {
                tableRevision = revision;
                return this;
            }

            /**
             * Add the {@code revision} to the {@link Receipt}.
             * 
             * @param revision
             * @return this
             */
            Builder itemize(IndexRevision revision) {
                indexRevision = revision;
                return this;
            }
        }

    }

    /**
     * Internal wrapper around an object that is {@link Freezable} and its
     * {@link ByteBuffer binary} representation.
     *
     * @author jeff
     */
    private static class Part implements Freezable {

        /**
         * The thing that can be frozen.
         */
        private final Freezable freezable;

        /**
         * The bytes for the frozen thing.
         */
        private final ByteBuffer bytes;

        /**
         * Construct a new instance.
         * 
         * @param freezable
         * @param bytes
         */
        public Part(Freezable freezable, ByteBuffer bytes) {
            this.freezable = freezable;
            this.bytes = bytes;
        }

        @Override
        public void freeze(Path file, long position) {
            freezable.freeze(file, position);
        }

        @Override
        public boolean isFrozen() {
            return freezable.isFrozen();
        }

        /**
         * Return the {@link #bytes}.
         * 
         * @return the {@link #bytes}
         */
        public ByteBuffer bytes() {
            return bytes;
        }

    }
}
