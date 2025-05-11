/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import static com.cinchapi.concourse.server.GlobalState.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.collect.concurrent.ThreadFactories;
import com.cinchapi.common.concurrent.JoinableExecutorService;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.collect.BridgeSortMap;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.concurrent.NoOpScheduledExecutorService;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.TObjectSorter;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.DurableStore;
import com.cinchapi.concourse.server.storage.Memory;
import com.cinchapi.concourse.server.storage.WriteStreamProfiler;
import com.cinchapi.concourse.server.storage.db.compaction.Compactor;
import com.cinchapi.concourse.server.storage.db.compaction.NoOpCompactor;
import com.cinchapi.concourse.server.storage.db.compaction.similarity.SimilarityCompactor;
import com.cinchapi.concourse.server.storage.db.kernel.CorpusArtifact;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.db.kernel.Segment.Receipt;
import com.cinchapi.concourse.server.storage.db.kernel.SegmentLoadingException;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.util.Comparators;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.TStrings;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeMultimap;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The {@link Database} is the {@link Engine Engine's} {@link DurableStore}
 * for data. The Database accepts {@link Write} objects that are initially
 * stored in a {@link Buffer} and converts them {@link Revision Revisions} that
 * are stored within distinct {@link Segment Segments}. Each {@link Segment} is
 * broken up into {@link Chunk Chunks} that provided optimized read-views.
 * <p>
 * Conceptually, the {@link Database} is a collection of three sparse, but
 * contiguous data repositories:
 * <ul>
 * <li>a <strong>table</strong> that contains a normalized view of data (similar
 * to an RDBMS table),
 * <li>an <strong>index</strong> that contains an inverted view of data (similar
 * to an RDBMS index), and
 * <li>a <strong>corpus</strong> that contains a searchable view of
 * {@link Value#isCharSequenceType() string-like} data
 * </ul>
 * While these conceptual repositories aren't actually maintained, the
 * {@link Revision Revisions} stored across the {@link Segment Segments} allows
 * for the ad-hoc accumulation of {@link TableRecord TableRecords},
 * {@link IndexRecord IndexRecords}, and {@link CorpusRecord CorpusRecords} to
 * service read requests efficiently.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class Database implements DurableStore {

    /**
     * Return a {@link ThreadFactory} that produces threads to run compaction
     * jobs.
     * 
     * @param environment
     * @param compactionType
     * @return the {@link ThreadFactory}
     */
    private static ThreadFactory createCompactionThreadFactory(
            String environment, String compactionType) {
        ThreadFactoryBuilder factory = new ThreadFactoryBuilder();
        factory.setDaemon(true);
        factory.setNameFormat(AnyStrings.format("{}Compaction [{}]",
                compactionType, environment));
        factory.setPriority(Thread.MIN_PRIORITY);
        factory.setUncaughtExceptionHandler((thread, exception) -> {
            Logger.error("Uncaught exception in {}:", thread.getName(),
                    exception);
            Logger.error(
                    "{} has STOPPED WORKING due to an unexpected exception. {} compaction is paused until the error is resolved",
                    thread.getName(), compactionType);
        });
        return factory.build();
    }

    /**
     * Return the {@link Segment} identified by {@code id} if it exists within
     * the collection of {@code segments}. If it doesn't return {@code null}.
     * 
     * @param segments
     * @param id
     * @return the {@link Segment} identified by {@code id} or {@code null}
     */
    @Nullable
    private static Segment findSegment(Collection<Segment> segments,
            String id) {
        for (Segment segment : segments) {
            if(segment.id().equals(id)) {
                return segment;
            }
        }
        return null;
    }

    /**
     * Global flag to indicate if async data reads are enabled
     */
    // Copied here as a final variable for (hopeful) performance gains.
    private static final boolean ENABLE_ASYNC_DATA_READS = GlobalState.ENABLE_ASYNC_DATA_READS;

    /**
     * Global flag that indicates if compaction is enabled.
     */
    // Copied here as a final variable for (hopeful) performance gains.
    private static final boolean ENABLE_COMPACTION = GlobalState.ENABLE_COMPACTION;

    /**
     * Global flag that indicates if search data is cached.
     */
    // Copied here as a final variable for (hopeful) performance gains.
    private static final boolean ENABLE_SEARCH_CACHE = GlobalState.ENABLE_SEARCH_CACHE;

    /**
     * Global flag that indicates if {@link #verify(String, TObject, long)} uses
     * {@link #getLookupRecord(Identifier, Text, Value) lookup records}.
     */
    // Copied here as a final variable for (hopeful) performance gains.
    private static final boolean ENABLE_VERIFY_BY_LOOKUP = GlobalState.ENABLE_VERIFY_BY_LOOKUP;

    /**
     * The initial number of seconds to wait, after the {@link Database}
     * {@link #start() starts} to run a full compaction.
     */
    private static long FULL_COMPACTION_INITIAL_DELAY_IN_SECONDS = TimeUnit.SECONDS
            .convert(1, TimeUnit.DAYS);

    /**
     * The number of seconds to wait in between full compactions.
     */
    private static long FULL_COMPACTION_RUN_FREQUENCY_IN_SECONDS = TimeUnit.SECONDS
            .convert(7, TimeUnit.DAYS);

    /**
     * The initial number of seconds to wait, after the {@link Database}
     * {@link #start() starts} to run an incremental compaction.
     */
    private static long INCREMENTAL_COMPACTION_INITIAL_DELAY_IN_SECONDS = 30;

    /**
     * The number of seconds to wait in between incremental compactions.
     */
    private static long INCREMENTAL_COMPACTION_RUN_FREQUENCY_IN_SECONDS = 2;

    /**
     * The subdirectory of {@link #directory} where the {@link Segment} files
     * are stored.
     */
    private static final String SEGMENTS_SUBDIRECTORY = "segments";

    /**
     * The {@link Compactor} that performs compaction.
     */
    private transient Compactor compactor;

    /**
     * Corpus Cache
     * <hr />
     * <p>
     * Caching for {@link CorpusRecord CorpusRecords} are segmented by key. This
     * is done in an attempt to avoid attempting cache updates for every infix
     * of a value when it is known that no search caches exist for the key from
     * which the value is mapped (e.g. we are indexing a term for a key that
     * isn't being searched).
     * </p>
     */
    private final Map<Text, LoadingCache<Composite, CorpusRecord>> corpusCaches = ENABLE_SEARCH_CACHE
            ? new ConcurrentHashMap<>()
            : ImmutableMap.of();

    /**
     * The location where the Database stores data.
     */
    private final transient Path directory;

    /**
     * Runs full compaction in the background.
     */
    private transient ScheduledExecutorService fullCompaction;

    /**
     * Runs incremental compaction in the background.
     */
    private transient ScheduledExecutorService incrementalCompaction;

    /**
     * Index Cache
     * <hr />
     * <p>
     * Records are cached in memory to reduce the number of seeks required. When
     * writing new revisions, we check the appropriate caches for relevant
     * records and append the new revision so that the cached data doesn't grow
     * stale.
     * </p>
     * <p>
     * The caches are only populated if the Database is #running (see
     * #accept(Write)). Attempts to get a Record when the Database is not
     * running will ignore the cache by virtue of an internal wrapper that has
     * the appropriate detection.
     * </p>
     */
    private final LoadingCache<Composite, IndexRecord> indexCache;

    // @formatter:off
    /**
     * The read/write lock that governs concurrent access to the Database's structure.
     *
     * <h3>Write Lock:</h3>
     * <ul>
     * <li>{@link #rotate(boolean)}/{@link #sync()}: Flushing the current seg0 and 
     *     installing a new one</li>
     * <li>{@link #merge(Segment, List)}: Inserting new Segments (typically during a
     *     {@link com.cinchapi.concourse.server.storage.transporter.Transporter#transport()
     *     transport} operation)</li>
     * <li>{@link #reindex()}: Rebuilding all Segment files atomically</li>
     * <li>{@link #repair()}: Deduplicating or garbage-collecting old Segment 
     *     files</li>
     * <li>{@link #compact() compaction}: Passed to the {@link Compactor} via
     *     {@link Storage} and acquired before every shift operation</li>
     * </ul>
     *
     * <h3>Read Lock:</h3>
     * The read-lock ensures the structure of the {@link #segments} remain consistent
     * when loading {@link Record Records}. When a Record is already cached, there is
     * no need to consult the {@link #segments}, but if it is unchached, the lock is 
     * held while seeking revisions, then released <strong>before</strong> the read is
     * issued. Read integrity is maintained even after the lock is released because new 
     * revisions are only added through {@link #accept(Write)} or {@link #merge(Segment, List)}, 
     * which update the thread-safe cached records via {@link #updateCaches(Receipt)}.
     */
    // @formatter:on
    private final transient ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    /**
     * A live view into the {@link Database Database's} memory.
     */
    private CacheState memory;

    /**
     * A flag to indicate if the Buffer is running or not.
     */
    private transient boolean running = false;

    /**
     * A {@link JoinableExecutorService} that facilitates
     * {@link #ENABLE_ASYNC_DATA_READS async data reading} if enabled.
     */
    private transient JoinableExecutorService reader;

    /**
     * We hold direct references to the current Segment. This pointer changes
     * whenever the database triggers a sync operation.
     */
    private transient Segment seg0;

    /**
     * <p>
     * A collection of all the segments, in manually sorted chronological order,
     * so that we can seek for the necessary revisions and populate a requested
     * record.
     * </p>
     * 
     * <p>
     * <strong>NOTE:</strong> We maintain the #segments in a List (instead
     * of a SortedSet) because a newly added Segment is always "greater" than an
     * existing Segment. The only time Segments are not added in monotonically
     * increasing order is when they are loaded when the database #start()s.
     * </p>
     */
    private final transient List<Segment> segments = new ArrayList<>();

    /**
     * The underlying {@link Storage}.
     */
    private final transient Storage storage;

    /**
     * Table Cache
     * <hr />
     * <p>
     * Records are cached in memory to reduce the number of seeks required. When
     * writing new revisions, we check the appropriate caches for relevant
     * records and append the new revision so that the cached data doesn't grow
     * stale.
     * </p>
     * <p>
     * The caches are only populated if the Database is #running (see
     * #accept(Write)). Attempts to get a Record when the Database is not
     * running will ignore the cache by virtue of an internal wrapper that has
     * the appropriate detection.
     * </p>
     */
    private final LoadingCache<Composite, TableRecord> tableCache;

    /**
     * Partial Table Cache
     * <hr />
     * <p>
     * Records are cached in memory to reduce the number of seeks required. When
     * writing new revisions, we check the appropriate caches for relevant
     * records and append the new revision so that the cached data doesn't grow
     * stale.
     * </p>
     * <p>
     * The caches are only populated if the Database is #running (see
     * #accept(Write)). Attempts to get a Record when the Database is not
     * running will ignore the cache by virtue of an internal wrapper that has
     * the appropriate detection.
     * </p>
     */
    private final LoadingCache<Composite, TableRecord> tablePartialCache;

    /**
     * A "tag" used to identify the Database's affiliations (e.g. environment).
     */
    private transient String tag = "";

    /**
     * An {@link ExecutorService} that is used for asynchronous writing tasks in
     * mutable {@link Segment segments}.
     */
    private transient AwaitableExecutorService writer;

    /**
     * Dynamic configuration.
     */
    private transient Options options = new Options();

    /**
     * Configuration that is used when {@link #buildCache(int, Function)
     * building caches}.
     */
    private final transient CacheConfiguration cacheConfig;

    /**
     * Construct a Database that is backed by the default location which is in
     * {@link GlobalState#DATABASE_DIRECTORY}.
     * 
     */
    public Database() {
        this(DATABASE_DIRECTORY);
    }

    /**
     * Construct a Database that is backed by {@link backingStore} directory.
     * The {@link backingStore} is passed to each {@link Record} as the
     * {@code parentStore}.
     * 
     * @param directory
     */
    public Database(Path directory) {
        this(directory, CacheConfiguration.defaults());
    }

    /**
     * Construct a Database that is backed by {@link backingStore} directory.
     * The {@link backingStore} is passed to each {@link Record} as the
     * {@code parentStore}.
     * 
     * @param directory
     */
    public Database(String directory) {
        this(Paths.get(directory));
    }

    /**
     * Construct a Database that is backed by {@link backingStore} directory.
     * The {@link backingStore} is passed to each {@link Record} as the
     * {@code parentStore}.
     * 
     * @param directory
     * @param cacheConfig
     */
    @SuppressWarnings("unchecked")
    Database(Path directory, CacheConfiguration cacheConfig) {
        this.directory = directory;
        this.storage = new Storage(directory.resolve(SEGMENTS_SUBDIRECTORY),
                segments, masterLock.writeLock());
        this.cacheConfig = cacheConfig;

        this.tableCache = buildCache(cacheConfig.tableCacheMemoryLimit(),
                composite -> {
                    Identifier identifier = (Identifier) composite.parts()[0];
                    TableRecord $ = TableRecord.create(identifier);
                    if(options.enableAsyncTableDataReads()) {
                        int i = 0;
                        Fragment<Identifier, Text, Value>[] fragments = new Fragment[segments
                                .size()];
                        Runnable[] tasks = new Runnable[segments.size()];
                        for (Segment segment : segments) {
                            Fragment<Identifier, Text, Value> fragment = new Fragment<>(
                                    identifier, null);
                            fragments[i] = fragment;
                            tasks[i++] = () -> segment.table().seek(composite,
                                    fragment);
                        }
                        reader.join(tasks);
                        $.append(fragments);
                    }
                    else {
                        for (Segment segment : segments) {
                            segment.table().seek(composite, $);
                        }
                    }
                    return $;
                });

        this.tablePartialCache = buildCache(
                cacheConfig.tablePartialCacheMemoryLimit(), composite -> {
                    Identifier identifier = (Identifier) composite.parts()[0];
                    Text key = (Text) composite.parts()[1];
                    TableRecord $ = TableRecord.createPartial(identifier, key);
                    if(options.enableAsyncTableDataReads()) {
                        int i = 0;
                        Fragment<Identifier, Text, Value>[] fragments = new Fragment[segments
                                .size()];
                        Runnable[] tasks = new Runnable[segments.size()];
                        for (Segment segment : segments) {
                            Fragment<Identifier, Text, Value> fragment = new Fragment<>(
                                    identifier, key);
                            fragments[i] = fragment;
                            tasks[i++] = () -> segment.table().seek(composite,
                                    fragment);
                        }
                        reader.join(tasks);
                        $.append(fragments);
                    }
                    else {
                        for (Segment segment : segments) {
                            segment.table().seek(composite, $);
                        }
                    }
                    return $;
                });

        this.indexCache = buildCache(cacheConfig.indexCacheMemoryLimit(),
                composite -> {
                    Text key = (Text) composite.parts()[0];
                    IndexRecord $ = IndexRecord.create(key);
                    if(options.enableAsyncIndexDataReads()) {
                        int i = 0;
                        Fragment<Text, Value, Identifier>[] fragments = new Fragment[segments
                                .size()];
                        Runnable[] tasks = new Runnable[segments.size()];
                        for (Segment segment : segments) {
                            Fragment<Text, Value, Identifier> fragment = new Fragment<>(
                                    key, null);
                            fragments[i] = fragment;
                            tasks[i++] = () -> segment.index().seek(composite,
                                    fragment);
                        }
                        reader.join(tasks);
                        $.append(fragments);
                    }
                    else {
                        for (Segment segment : segments) {
                            segment.index().seek(composite, $);
                        }
                    }
                    return $;
                });
    }

    @Override
    public void accept(Write write) {
        // NOTE: This approach is thread safe because write locking happens
        // in each of #seg0's individual Chunks, and furthermore this method
        // is only called from the Buffer, which transports data serially.
        if(running) {
            try {
                Receipt receipt = seg0.acquire(write, writer);
                Logger.debug("Indexed '{}' in {}", write, seg0);

                updateCaches(receipt);
            }
            catch (InterruptedException e) {
                Logger.warn(
                        "The database was interrupted while trying to accept {}. "
                                + "If the write could not be fully accepted, it will "
                                + "remain in the buffer and re-tried when the Database is able to accept writes.",
                        write);
                Thread.currentThread().interrupt();
                return;
            }
        }
        else {
            // The #accept method may be called when the database is stopped
            // during test cases
            Logger.warn(
                    "The database is being asked to accept a Write, even though it is not running.");
            seg0.acquire(write);
        }
    }

    @Override
    public void accept(Write write, boolean sync) {
        // It is never necessary to sync after each Write since syncing is
        // coordinated by the Engine when a Buffer page has been depleted.
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Map<Value, Set<Identifier>> data = index.getAll();
        return Internals.transformAssumedSortedMap(data, Value::getTObject,
                Identifier::longValue, TObjectSorter.INSTANCE);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Map<Value, Set<Identifier>> data = index.getAll(timestamp);
        return Internals.transformAssumedSortedMap(data, Value::getTObject,
                Identifier::longValue, TObjectSorter.INSTANCE);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        Identifier L = Identifier.of(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L);
        Map<Long, Set<Value>> data = table.chronologize(K, start, end);
        return Transformers.transformMapSet(data, Functions.identity(),
                Value::getTObject);
    }

    @Override
    public void compact() {
        compactor.tryIncrementalCompaction();
    }

    @Override
    public boolean contains(long record) {
        Identifier L = Identifier.of(record);
        TableRecord table = getTableRecord(L);
        return !table.isEmpty();
    }

    /**
     * Return dumps for all the blocks identified by {@code id}. This method IS
     * NOT necessarily optimized for performance, so it should be used with
     * caution. Its only really necessary to use this method for debugging.
     * 
     * @param id
     * @return the block dumps.
     */
    public String dump(String id) {
        Segment segment = findSegment(segments, id);
        Preconditions.checkArgument(segment != null,
                "No segment identified by %s", id);
        StringBuilder sb = new StringBuilder();
        sb.append(segment.table().dump());
        sb.append(segment.index().dump());
        sb.append(segment.corpus().dump());
        return sb.toString();
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Value[] Ks = Transformers.transformArray(aliases.values(), Value::wrap,
                Value.class);
        Map<Identifier, Set<Value>> map = index.findAndGet(aliases.operator(),
                Ks);
        return Transformers.transformTreeMapSet(map, Identifier::longValue,
                Value::getTObject, Long::compare);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases,
            long timestamp) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Value[] Ks = Transformers.transformArray(aliases.values(), Value::wrap,
                Value.class);
        Map<Identifier, Set<Value>> map = index.findAndGet(timestamp,
                aliases.operator(), Ks);
        return Transformers.transformTreeMapSet(map, Identifier::longValue,
                Value::getTObject, Long::compare);
    }

    @Override
    public Set<TObject> gather(String key, long record) {
        Text L = Text.wrapCached(key);
        Identifier V = Identifier.of(record);
        IndexRecord index = getIndexRecord(L);
        Set<Value> Ks = index.gather(V);
        return Transformers.transformSet(Ks, Value::getTObject);
    }

    @Override
    public Set<TObject> gather(String key, long record, long timestamp) {
        Text L = Text.wrapCached(key);
        Identifier V = Identifier.of(record);
        IndexRecord index = getIndexRecord(L);
        Set<Value> Ks = index.gather(V, timestamp);
        return Transformers.transformSet(Ks, Value::getTObject);
    }

    /**
     * Return the location where the Database stores its data.
     * 
     * @return the backingStore
     */
    @Restricted
    public String getBackingStore() {
        return directory.toString();
    }

    /**
     * Return a the list of ids for all the blocks that are currently in scope.
     * 
     * @return the block dump list
     */
    @ManagedOperation
    public List<String> getDumpList() {
        return segments.stream().map(Segment::id).collect(Collectors.toList());
    }

    /**
     * Return an {@link Iterator} that provides access to all the
     * {@link Write Writes} that have been {@link #accept(Write)
     * accepted}.
     * 
     * @return an {@link Iterator} over accepted {@link Write Writes}.
     */
    public Iterator<Write> iterator() {
        return new AcceptedWriteIterator();
    }

    @Override
    public Memory memory() {
        Verify.that(running,
                "Cannot return the memory of a stopped Database instance");
        return memory;
    }

    /**
     * Merge a {@link Segment} that contains data that was not previously
     * {@link #accept(Write) accepted} by the {@link Database}.
     * <p>
     * In addition to appending the {@code segment}, this method ensures that
     * any necessary caches are {@link #updateCaches(Receipt) updated}.
     * And, if the {@code segment} is {@link Segment#isMutable() mutable}, it
     * will be {@link SegmentStorageSystem#save(Segment) persisted} to disk and
     * made immutable.
     * </p>
     * <p>
     * <strong>NOTE:</strong> This method is only intended for adding net-new
     * data that changes the "state" that should be observed by future reads.
     * For operations where segments are being inserted but "state" is not
     * changing (e.g., because the segment contains duplicate data), use the
     * appropriate specialized method instead:
     * <ul>
     * <li>For reindexing, use the {@link #reindex()} method</li>
     * <li>For compaction, use the {@link #compact()} method or a
     * {@link Compactor}</li>
     * </ul>
     * <em>Those methods manage segment files without modifying the caches in
     * the {@link Database}.</em>
     * </p>
     * 
     * @param segment the {@link Segment} to merge into the database
     * @param receipts a {@link List} of {@link Receipt receipts} representing
     *            all the {@link Segment#acquire(Write) acquisitions} by the
     *            {@link Segment}
     */
    public boolean merge(Segment segment, List<Receipt> receipts) {
        masterLock.writeLock().lock();
        try {
            String id = segment.id();

            // By convention, #seg0 must always be at the end of the list
            // because it is Segment that acquires #accepted Writes.
            int index = Math.max(segments.size() - 1, 0);
            segments.add(index, segment);

            if(segment.isMutable()) {
                Path file = storage.save(segment);
                Logger.debug("Completed sync of {} to disk at {}", id, file);
            }

            for (Receipt receipt : receipts) {
                updateCaches(receipt);
            }

            Logger.debug("Completed merge of {} to the Database", id);
            return true;
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    @Override
    public void reconcile(Set<HashCode> hashes) {
        Logger.info("Reconciling the states of the Database and Buffer...");
        // CON-83, GH-441, GH-442: Check for premature shutdown or crash that
        // regenerated Segment files based on Write versions that are all still
        // in the buffer.
        if(segments.size() > 1) {
            int index = segments.size() - 2;
            Segment seg1 = segments.get(index);
            if(hashes.containsAll(seg1.hashes())) {
                Logger.warn(
                        "The data in {} is still completely in the BUFFER so it is being discarded",
                        seg1);
                segments.remove(index);
            }
        }
    }

    /**
     * {@link Segment#reindex() Reindex} every {@link Segment} in the
     * {@link Database}.
     * <p>
     * This operation will block reads and writes until it finishes.
     * </p>
     */
    public void reindex() {
        // NOTE: Reindexing must be single threaded because there are a fixed
        // number of dedicated threads dedicated for searching indexing (see
        // CorpusChunk) and multiple threads enqueing here, would cause
        // thrashing.
        masterLock.writeLock().lock();
        try {
            for (int i = 0; i < segments.size(); ++i) {
                Segment current = segments.get(i);
                Segment updated = current.reindex();
                if(current == seg0) {
                    seg0 = updated;
                }
                else {
                    storage.save(updated);
                    current.delete();
                }
                segments.set(i, updated);
                Logger.info(
                        "Reindexed Segment {}. The data is now available in Segment {}",
                        current.id(), updated);
            }
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    @Override
    public void repair() {
        masterLock.writeLock().lock();
        try {
            WriteStreamProfiler<Segment> profiler = new WriteStreamProfiler<>(
                    segments);

            // Deduplication - Remove duplicate Writes about the same topic at
            // the same commit version (usually happens when the server is
            // stopped before a Transaction commit is fully acknowledged)
            Map<Segment, Segment> deduped = profiler
                    .deduplicate(() -> Segment.create());
            if(!deduped.isEmpty()) {
                for (int i = 0; i < segments.size(); ++i) {
                    Segment segment = segments.get(i);
                    Segment clean = deduped.get(segment);
                    if(clean != null) {
                        storage.save(clean);
                        segments.set(i, clean);
                        segment.delete();
                    }
                }
                int total = profiler.duplicates().size();
                Logger.warn(
                        "Replaced {} Segments that contained duplicate data. In total, across all Segments, there were {} Write{} duplicated.",
                        deduped.size(), total, total != 1 ? "s" : "");
            }

            // Balancing - Remove Writes that should not have been inserted
            // because they did not properly offset a prior Write about the same
            // topic. It's unclear why this can happen, but if it does, it
            // leaves the Database in an invalid state. Removing these Writes
            // does not represent a loss of data because the intended state does
            // not change
            Map<Segment, Segment> balanced = profiler
                    .balance(() -> Segment.create());
            if(!balanced.isEmpty()) {
                for (int i = 0; i < segments.size(); ++i) {
                    Segment segment = segments.get(i);
                    Segment clean = balanced.get(segment);
                    if(clean != null) {
                        clean.transfer(storage.directory()
                                .resolve(UUID.randomUUID() + ".seg"));
                        segments.set(i, clean);
                        segment.delete();
                    }
                }
                Logger.warn(
                        "Replaced {} Segments that contained unoffset Writes.",
                        balanced.size());
            }
        }
        finally {
            masterLock.writeLock().unlock();
        }

    }

    @Override
    public Map<Long, List<String>> review(long record) {
        Identifier L = Identifier.of(record);
        TableRecord table = getTableRecord(L);
        return table.review();
    }

    @Override
    public Map<Long, List<String>> review(String key, long record) {
        Identifier L = Identifier.of(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L, K);
        return table.review(K);
    }

    @Override
    public Set<Long> search(String key, String query) {
        // NOTE: Locking must happen here since CorpusRecords are not cached and
        // search potentially works across multiple ones.
        masterLock.readLock().lock();
        try {
            Text L = Text.wrapCached(key);
            // Get each word in the query separately to ensure that multi word
            // search works.
            String[] words = query.toString().toLowerCase().split(
                    TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
            Multimap<Identifier, Integer> reference = ImmutableMultimap.of();
            boolean initial = true;
            for (String word : words) {
                Text K = Text.wrap(word);
                CorpusRecord corpus = getCorpusRecord(L, K);
                Set<Position> appearances = corpus.get(K);
                Multimap<Identifier, Integer> temp = HashMultimap.create();
                for (Position appearance : appearances) {
                    Identifier record = appearance.getIdentifier();
                    int position = appearance.getIndex();
                    if(initial) {
                        temp.put(record, position);
                    }
                    else {
                        for (int current : reference.get(record)) {
                            if(position == current + 1) {
                                temp.put(record, position);
                            }
                        }
                    }
                }
                initial = false;
                reference = temp;
            }

            // Result Scoring: Scoring is simply the number of times the query
            // appears in a Record [e.g. the number of Positions mapped from
            // key: #reference.get(key).size()]. The total number of positions
            // in #reference is equal to the total number of times a document
            // appears in the corpus [e.g. reference.asMap().values().size()].
            Multimap<Integer, Long> sorted = TreeMultimap.create(
                    Collections.<Integer> reverseOrder(),
                    Long::compareUnsigned);
            for (Entry<Identifier, Collection<Integer>> entry : reference
                    .asMap().entrySet()) {
                sorted.put(entry.getValue().size(), entry.getKey().longValue());
            }
            Set<Long> results = (Set<Long>) sorted.values().stream()
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return results;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        Identifier L = Identifier.of(record);
        TableRecord table = getTableRecord(L);
        Map<Text, Set<Value>> data = table.getAll();
        return Transformers.transformTreeMapSet(data, Text::toString,
                Value::getTObject,
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        Identifier L = Identifier.of(record);
        TableRecord table = getTableRecord(L);
        Map<Text, Set<Value>> data = table.getAll(timestamp);
        return Transformers.transformTreeMapSet(data, Text::toString,
                Value::getTObject,
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        Identifier L = Identifier.of(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L, K);
        Set<Value> data = table.get(K);
        return Transformers.transformSet(data, Value::getTObject);
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp) {
        Identifier L = Identifier.of(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L, K);
        Set<Value> data = table.get(K, timestamp);
        return Transformers.transformSet(data, Value::getTObject);
    }

    @Override
    public void start() {
        if(!running) {
            Logger.info("Database configured to store data in {}", directory);
            int numWorkers = Math.max(3,
                    Runtime.getRuntime().availableProcessors());
            running = true;
            this.writer = new AwaitableExecutorService(
                    Executors.newFixedThreadPool(numWorkers, ThreadFactories
                            .namingThreadFactory("DatabaseWriter")));
            this.segments.clear();
            ArrayBuilder<Runnable> tasks = ArrayBuilder.builder();
            List<Segment> segments = Collections
                    .synchronizedList(this.segments);
            Stream<Path> files = storage.files();
            files.forEach(file -> tasks.add(() -> {
                try {
                    Segment segment = Segment.load(file);
                    segments.add(segment);
                }
                catch (SegmentLoadingException e) {
                    Logger.error("Error when trying to load Segment {}", file);
                    Logger.error("", e);
                }
            }));
            files.close();
            if(tasks.length() > 0) {
                AwaitableExecutorService loader = new AwaitableExecutorService(
                        Executors.newFixedThreadPool(numWorkers, ThreadFactories
                                .namingThreadFactory("DatabaseLoader")));
                try {
                    loader.await((task, error) -> Logger.error(
                            "Unexpected error when trying to load Database Segments: {}",
                            error), tasks.build());
                }
                catch (InterruptedException e) {
                    Logger.error(
                            "The Database was interrupted while starting...",
                            e);
                    Thread.currentThread().interrupt();
                    return;
                }
                finally {
                    loader.shutdown();
                }
            }

            // Sort the segments in chronological order
            Collections.sort(this.segments, Segment.TEMPORAL_COMPARATOR);

            // Remove segments that overlap. Segments may overlap if they are
            // duplicates resulting from a botched upgrade or reindex or if they
            // were involved in an optimization pass, but garbage collection
            // didn't run before the server shutdown.
            ListIterator<Segment> lit = segments.listIterator();
            while (lit.hasNext()) {
                if(lit.hasPrevious()) {
                    Segment previous = lit.previous();
                    lit.next();
                    Segment current = lit.next();
                    if(current.intersects(previous)) {
                        lit.previous();
                        lit.previous();
                        lit.remove();
                        Logger.warn(
                                "Segment {} was not loaded because it contains duplicate data. It has been scheduled for garbage collection.",
                                previous);
                        // TODO: mark #previous for garbage collection
                    }
                }
                else {
                    lit.next();
                }
            }

            rotate(false);
            memory = new CacheState();

            /*
             * If enabled, setup Compaction to run continuously in the
             * background; trying to perform both "full" and "incremental"
             * compaction. Incremental compaction is opportunistic; attempting
             * frequently, but only occurring if no other conflicting work is
             * happening and only trying to compact one "shift". On the other
             * hand,full compaction runs less frequently, but is very
             * aggressive: blocking until any other conflicting work is done
             * and trying every possible shift.
             */
            // @formatter:off
            compactor = ENABLE_COMPACTION
                    ? new SimilarityCompactor(storage)
                    : NoOpCompactor.instance();

            fullCompaction = ENABLE_COMPACTION
                    ? Executors.newScheduledThreadPool(1,
                            createCompactionThreadFactory(tag, "Full"))
                    : NoOpScheduledExecutorService.instance();
            fullCompaction.scheduleWithFixedDelay(
                    () -> compactor.executeFullCompaction(),
                    FULL_COMPACTION_INITIAL_DELAY_IN_SECONDS,
                    FULL_COMPACTION_RUN_FREQUENCY_IN_SECONDS,
                    TimeUnit.SECONDS);

            incrementalCompaction = ENABLE_COMPACTION
                    ? Executors.newScheduledThreadPool(1,
                            createCompactionThreadFactory(tag, "Incremental"))
                    : NoOpScheduledExecutorService.instance();
            incrementalCompaction.scheduleWithFixedDelay(
                    () -> compactor.tryIncrementalCompaction(),
                    INCREMENTAL_COMPACTION_INITIAL_DELAY_IN_SECONDS,
                    INCREMENTAL_COMPACTION_RUN_FREQUENCY_IN_SECONDS,
                    TimeUnit.SECONDS);
            // @formatter:on
            Logger.info("Database is running with compaction {}.",
                    ENABLE_COMPACTION ? "ON" : "OFF");

            reader = ENABLE_ASYNC_DATA_READS ? new JoinableExecutorService(
                    Math.max(3, Runtime.getRuntime().availableProcessors()),
                    ThreadFactories.namingThreadFactory("DatabaseReader"))
                    : null;
        }

    }

    @Override
    public void stop() {
        if(running) {
            running = false;
            writer.shutdown();
            memory = null;
            Streams.concat(ImmutableList
                    .of(tableCache, tablePartialCache, indexCache).stream(),
                    corpusCaches.values().stream()).forEach(cache -> {
                        cache.invalidateAll();
                    });
            for (Segment segment : segments) {
                try {
                    segment.close();
                }
                catch (IOException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }
            fullCompaction.shutdownNow();
            incrementalCompaction.shutdownNow();
            if(reader != null) {
                reader.shutdown();
            }
        }
    }

    @Override
    public void sync() {
        rotate(true);
    }

    /**
     * Set the {@link Database Database's} tag.
     * 
     * @param tag
     */
    public void tag(String tag) {
        this.tag = tag;
    }

    @Override
    public boolean verify(Write write) {
        Identifier L = write.getRecord();
        Text K = write.getKey();
        Value V = write.getValue();
        Record<Identifier, Text, Value> table = ENABLE_VERIFY_BY_LOOKUP
                ? getLookupRecord(L, K, V)
                : getTableRecord(L, K);
        return table.contains(K, V);
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        Identifier L = write.getRecord();
        Text K = write.getKey();
        Value V = write.getValue();
        TableRecord table = getTableRecord(L, K);
        return table.contains(K, V, timestamp);
    }

    /**
     * Build a {@link LoadingCache cache} for {@link Record Records} that are
     * loaded from disk. The returned cache will use the specified {#code
     * loader} to read the appropriate data from disk and populate the
     * {@link Record} accordingly.
     * 
     * @param maximumWeight
     * @param loader
     * 
     * @return the cache
     */
    private <T extends Record<?, ?, ?>> LoadingCache<Composite, T> buildCache(
            int maximumWeight, Function<Composite, T> loader) {
        Weigher<Composite, T> weigher = (key, value) -> key.size()
                + value.size();
        // @formatter:off
        LoadingCache<Composite, T> cache = CacheBuilder.newBuilder()
                .weigher(weigher)
                .maximumWeight(maximumWeight)
                .softValues()
                .removalListener(notification -> {
                    if(notification.wasEvicted()) {
                        T record = notification.getValue();
                        Logger.debug("Evicted {} from cache due to {}", 
                                record, notification.getCause());
                        
                        cacheConfig.evictionListener().accept(record);
                    }
                })
                .refreshAfterWrite(
                        cacheConfig.cacheMemoryCheckFrequencyInSeconds(),TimeUnit.SECONDS)
                .build(new CacheLoader<Composite, T>() {

                    @Override
                    public T load(Composite key) throws Exception {
                        return loader.apply(key);
                    }

                    @Override
                    public ListenableFuture<T> reload(Composite key, T oldValue)
                            throws Exception {
                        // In order to maintain the configured maximumWeight,
                        // cached records are "reloaded" periodically after they
                        // are stored. Internally, each Record keeps a running
                        // total of its weight, so there's no need to re-apply
                        // the #loader to refresh the cached value or the cache
                        // metadata (e.g., running total weight)
                        return Futures.immediateFuture(oldValue);
                    }

                });
        // @formatter:on
        return new RunningAwareCache<>(cache, loader);
    }

    /**
     * Return the CorpusRecord identified by {@code key}.
     * 
     * @param key
     * @param query
     * @param toks {@code query} split by whitespace
     * @return the CorpusRecord
     */
    @SuppressWarnings("unchecked")
    private CorpusRecord getCorpusRecord(Text key, Text infix) {
        masterLock.readLock().lock();
        try {
            Function<Composite, CorpusRecord> loader = composite -> {
                Text $key = (Text) composite.parts()[0];
                Text $infix = (Text) composite.parts()[1];
                CorpusRecord $ = CorpusRecord.createPartial($key, $infix);
                if(options.enableAsyncCorpusDataReads()) {
                    int i = 0;
                    Fragment<Text, Text, Position>[] fragments = new Fragment[segments
                            .size()];
                    Runnable[] tasks = new Runnable[segments.size()];
                    for (Segment segment : segments) {
                        Fragment<Text, Text, Position> fragment = new Fragment<>(
                                $key, $infix);
                        fragments[i] = fragment;
                        tasks[i++] = () -> segment.corpus().seek(composite,
                                fragment);
                    }
                    reader.join(tasks);
                    $.append(fragments);
                }
                else {
                    for (Segment segment : segments) {
                        segment.corpus().seek(composite, $);
                    }
                }
                return $;
            };
            Composite composite = Composite.create(key, infix);
            if(ENABLE_SEARCH_CACHE) {
                LoadingCache<Composite, CorpusRecord> cache = corpusCaches
                        .computeIfAbsent(key,
                                $ -> buildCache(
                                        cacheConfig.corpusCacheMemoryLimit(),
                                        loader));
                return cache.get(composite);
            }
            else {
                return loader.apply(composite);
            }
        }
        catch (ExecutionException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the IndexRecord identified by {@code key}.
     * 
     * @param key
     * @return the IndexRecord
     */
    private IndexRecord getIndexRecord(Text key) {
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(key);
            return indexCache.get(composite);
        }
        catch (ExecutionException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return a {@link Record} that is guaranteed to have the present state for
     * whether {@code value} is contained for {@code key} in {@code record}. The
     * truth of this query can be obtained using the
     * {@link Record#contains(com.cinchapi.concourse.server.io.Byteable, com.cinchapi.concourse.server.io.Byteable)}
     * method on the returned {@link Record}.
     * <p>
     * The query answered by this {@link Record} can also be answered by that
     * returned from {@link #getTableRecord(Identifier)}
     * and {@link #getTableRecord(Identifier, Text)}, but this method will
     * attempt to short circuit by not loading {@link Revisions} that don't
     * involve {@code record}, {@code key} and {@code value}. As a result, the
     * returned {@link Record} is not cached and cannot be reliably used for
     * other queries.
     * </p>
     * 
     * @param record
     * @param key
     * @param value
     * @return the {@link Record}
     */
    private Record<Identifier, Text, Value> getLookupRecord(Identifier record,
            Text key, Value value) {
        masterLock.readLock().lock();
        try {
            // First, see if there is a cached full or partial Record that can
            // allow a lookup to be performed.
            Composite c1 = Composite.create(record);
            Composite c2 = null;
            Composite c3 = null;
            Record<Identifier, Text, Value> lookup = tableCache
                    .getIfPresent(c1);
            if(lookup == null) {
                c2 = Composite.create(record, key);
                lookup = tablePartialCache.getIfPresent(c2);
            }
            if(lookup == null) {
                // Create a LookupRecord to handle this, but DO NOT cache it
                // since it has no other utility.
                c3 = Composite.create(record, key, value);
                lookup = new LookupRecord(record, key, value);
                for (Segment segment : segments) {
                    if(segment.table().mightContain(c3)) {
                        // Whenever it is possible that the LKV exists, we must
                        // gather Revisions for LK within a Record so the
                        // current state of LKV can be determined.
                        segment.table().seek(c2, lookup);
                    }
                }
            }
            return lookup;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the TableRecord identifier by {@code identifier}.
     * 
     * @param identifier
     * @return the TableRecord
     */
    private TableRecord getTableRecord(Identifier identifier) {
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(identifier);
            return tableCache.get(composite);
        }
        catch (ExecutionException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the potentially partial TableRecord identified by {@code key} in
     * {@code identifier}.
     * <p>
     * While the returned {@link TableRecord} may not be
     * {@link TableRecord#isPartial() partial}, the caller should interact
     * with it as if it is (e.g. do not perform reads for any other keys besides
     * {@code key}.
     * </p>
     * 
     * @param identifier
     * @param key
     * @return the TableRecord
     */
    private TableRecord getTableRecord(Identifier identifier, Text key) {
        masterLock.readLock().lock();
        try {
            // Before loading a partial record, see if the full record is
            // present in memory.
            TableRecord table = tableCache
                    .getIfPresent(Composite.create(identifier));
            if(table == null) {
                Composite composite = Composite.create(identifier, key);
                table = tablePartialCache.get(composite);
            }
            return table;
        }
        catch (ExecutionException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Rotate the database by adding a new {@link Segment} and setting it as
     * {@link #seg0} so that it is the destination into which subsequent
     * {@link Write Writes} are {@link #accept(Write) accepted}.
     * 
     * @param flush - a flag that controls whether the current {@link #seg0} is
     *            durably flushed to disk prior rotating; if this is
     *            {@code false} the data unflushed data will exist in memory as
     *            long as the server is running or until it is later flushed.
     *            Sometimes this method is called when there is no
     *            data to sync and the a mutable {@link Segment} needs to be
     *            created to accept {@link Write Writes} (e.g. on
     *            {@link #start()}).
     */
    private void rotate(boolean flush) {
        masterLock.writeLock().lock();
        try {
            if(flush) {
                String id = seg0.id();
                Path file = storage.save(seg0);
                Logger.debug("Completed sync of {} to disk at {}", id, file);
            }
            segments.add((seg0 = Segment.create()));
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    /**
     * If necessary, update all the in-memory caches with data from the
     * {@link Receipt receipt} in order to maintain read consistency.
     * <p>
     * <strong>NOTE:</strong> This method does not validate that the
     * {@link Receipt} came from one of the {@link Database database's}
     * {@link #segments}, so the caller must ensuring that the {@link Receipt}
     * contains valid data.
     * </p>
     * 
     * @param receipt the {@link Receipt} containing data that my need to be
     *            incorporated into one or more of the internal caches
     */
    private void updateCaches(Receipt receipt) {
        masterLock.writeLock().lock();
        try {
            Composite composite;

            composite = receipt.table().getLocatorComposite();
            TableRecord table = tableCache.getIfPresent(composite);
            if(table != null) {
                table.append(receipt.table().revision());
            }

            composite = receipt.table().getLocatorKeyComposite();
            TableRecord tablePartial = tablePartialCache
                    .getIfPresent(composite);
            if(tablePartial != null) {
                tablePartial.append(receipt.table().revision());
            }

            composite = receipt.index().getLocatorComposite();
            IndexRecord index = indexCache.getIfPresent(composite);
            if(index != null) {
                index.append(receipt.index().revision());
            }

            if(ENABLE_SEARCH_CACHE) {
                Text key = receipt.table().revision().getKey();
                Cache<Composite, CorpusRecord> cache = corpusCaches.get(key);
                if(cache != null) {
                    for (CorpusArtifact artifact : receipt.corpus()) {
                        composite = artifact.getLocatorKeyComposite();
                        CorpusRecord corpus = cache.getIfPresent(composite);
                        if(corpus != null) {
                            corpus.append(artifact.revision());
                        }
                    }
                }
            }
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    /**
     * Configuration for the various caches used by the {@link Database}.
     * <p>
     * This class provides settings that control the maximum size and refresh
     * frequency
     * for the table, index, and corpus caches.
     * </p>
     */
    static final class CacheConfiguration {

        /**
         * Return a new {@link Builder} for constructing a
         * {@link CacheConfiguration}.
         * 
         * @return the {@link Builder}
         */
        static Builder builder() {
            return new Builder();
        }

        /**
         * Return the default {@link CacheConfiguration}.
         * 
         * @return the {@link CacheConfiguration}
         */
        static CacheConfiguration defaults() {
            /*
             * TODO: Consider a future optimization where the allocation for
             * each cache is determined based on heuristics (e.g., 50% for
             * corpus cache, 25% for table, 25% for index, etc) or based or
             * dynamic based on usage.
             */
            int tableCacheMemoryLimit;
            int tablePartialCacheMemoryLimit;
            int indexCacheMemoryLimit;
            int corpusCacheMemoryLimit;
            int cacheMemoryCheckFrequencyInSeconds;

            if(ENABLE_SEARCH_CACHE) {
                tableCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;
                tablePartialCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;
                indexCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;
                corpusCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;

            }
            else {
                tableCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;
                tablePartialCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;
                indexCacheMemoryLimit = GlobalState.CACHE_MEMORY_LIMIT;
                corpusCacheMemoryLimit = 0;
            }
            cacheMemoryCheckFrequencyInSeconds = GlobalState.CACHE_MEMORY_CHECK_FREQUENCY;

            return new CacheConfiguration(tableCacheMemoryLimit,
                    tablePartialCacheMemoryLimit, indexCacheMemoryLimit,
                    corpusCacheMemoryLimit, cacheMemoryCheckFrequencyInSeconds,
                    $ -> {});
        }

        /**
         * The maximum size for the table cache.
         */
        private int tableCacheMemoryLimit;

        /**
         * The maximum size for the table partial cache.
         */
        private int tablePartialCacheMemoryLimit;

        /**
         * The maximum size for the index cache.
         */
        private int indexCacheMemoryLimit;

        /**
         * The maximum size for the corpus cache.
         */
        private int corpusCacheMemoryLimit;

        /**
         * The frequency in seconds at which cache sizes are refreshed.
         */
        private int cacheMemoryCheckFrequencyInSeconds;

        /**
         * The eviction listener for cache entries.
         */
        private Consumer<Record<?, ?, ?>> evictionListener;

        /**
         * Construct a new instance.
         * 
         * @param tableCacheMemoryLimit the maximum size for the table cache
         * @param tablePartialCacheMemoryLimit the maximum size for the table
         *            partial cache
         * @param indexCacheMemoryLimit the maximum size for the index cache
         * @param corpusCacheMemoryLimit the maximum size for the corpus cache
         * @param cacheMemoryCheckFrequencyInSeconds the frequency in seconds at
         *            which cache sizes are refreshed
         * @param evictionListener the listener for cache entry eviction events
         */
        private CacheConfiguration(int tableCacheMemoryLimit,
                int tablePartialCacheMemoryLimit, int indexCacheMemoryLimit,
                int corpusCacheMemoryLimit,
                int cacheMemoryCheckFrequencyInSeconds,
                Consumer<Record<?, ?, ?>> evictionListener) {
            this.tableCacheMemoryLimit = tableCacheMemoryLimit;
            this.tablePartialCacheMemoryLimit = tablePartialCacheMemoryLimit;
            this.indexCacheMemoryLimit = indexCacheMemoryLimit;
            this.corpusCacheMemoryLimit = corpusCacheMemoryLimit;
            this.cacheMemoryCheckFrequencyInSeconds = cacheMemoryCheckFrequencyInSeconds;
            this.evictionListener = evictionListener;
        }

        /**
         * Return the frequency in seconds at which cache sizes are refreshed.
         * 
         * @return the cache size refresh frequency in seconds
         */
        int cacheMemoryCheckFrequencyInSeconds() {
            return cacheMemoryCheckFrequencyInSeconds;
        }

        /**
         * Return the maximum size for the corpus cache.
         * 
         * @return the corpus cache maximum size
         */
        int corpusCacheMemoryLimit() {
            return corpusCacheMemoryLimit;
        }

        /**
         * Return the removal listener for cache entries.
         * 
         * @return the removal listener
         */
        Consumer<Record<?, ?, ?>> evictionListener() {
            return evictionListener;
        }

        /**
         * Return the maximum size for the index cache.
         * 
         * @return the index cache maximum size
         */
        int indexCacheMemoryLimit() {
            return indexCacheMemoryLimit;
        }

        /**
         * Return the maximum size for the table cache.
         * 
         * @return the table cache maximum size
         */
        int tableCacheMemoryLimit() {
            return tableCacheMemoryLimit;
        }

        /**
         * Return the maximum size for the table partial cache.
         * 
         * @return the table partial cache maximum size
         */
        int tablePartialCacheMemoryLimit() {
            return tablePartialCacheMemoryLimit;
        }

        /**
         * A builder for creating {@link CacheConfiguration} instances with
         * custom settings.
         *
         * @author Jeff Nelson
         */
        static final class Builder {

            /**
             * The {@link CacheConfiguration} being built.
             */
            private final CacheConfiguration cacheConfig;

            /**
             * Construct a new instance.
             */
            private Builder() {
                this.cacheConfig = defaults();
            }

            /**
             * Build and return the {@link CacheConfiguration}.
             * 
             * @return the built {@link CacheConfiguration}
             */
            CacheConfiguration build() {
                return cacheConfig;
            }

            /**
             * Set the maximum size for the corpus cache.
             * 
             * @param value the maximum size for the corpus cache
             * @return this builder
             */
            Builder corpusCacheMemoryLimit(int value) {
                cacheConfig.corpusCacheMemoryLimit = value;
                return this;
            }

            /**
             * Set the eviction listener for cache entries.
             * 
             * @param listener the removal listener to use
             * @return this builder
             */
            Builder evictionListener(Consumer<Record<?, ?, ?>> listener) {
                cacheConfig.evictionListener = listener;
                return this;
            }

            /**
             * Set the maximum size for the index cache.
             * 
             * @param value the maximum size for the index cache
             * @return this builder
             */
            Builder indexCacheMemoryLimit(int value) {
                cacheConfig.indexCacheMemoryLimit = value;
                return this;
            }

            /**
             * Set the frequency in seconds at which cache sizes are refreshed.
             * 
             * @param value the cache size refresh frequency in seconds
             * @return this builder
             */
            Builder memoryCheckFrequencyInSeconds(int value) {
                cacheConfig.cacheMemoryCheckFrequencyInSeconds = value;
                return this;
            }

            /**
             * Set the maximum size for all caches (table, table partial, index,
             * and corpus).
             * 
             * @param value the maximum size for all caches
             * @return this builder
             */
            Builder memoryLimit(int value) {
                cacheConfig.tableCacheMemoryLimit = value;
                cacheConfig.tablePartialCacheMemoryLimit = value;
                cacheConfig.indexCacheMemoryLimit = value;
                cacheConfig.corpusCacheMemoryLimit = value;
                return this;
            }

            /**
             * Set the maximum size for the table cache.
             * 
             * @param value the maximum size for the table cache
             * @return this builder
             */
            Builder tableCacheMemoryLimit(int value) {
                cacheConfig.tableCacheMemoryLimit = value;
                return this;
            }

            /**
             * Set the maximum size for the table partial cache.
             * 
             * @param value the maximum size for the table partial cache
             * @return this builder
             */
            Builder tablePartialCacheMemoryLimit(int value) {
                cacheConfig.tablePartialCacheMemoryLimit = value;
                return this;
            }
        }
    }

    /**
     * A "snapshot" iterator (e.g. changes to the {@link #segments} are not
     * visible) over {@link Write Writes} that have been accepted by the
     * {@link Database}.
     *
     *
     * @author Jeff Nelson
     */
    private final class AcceptedWriteIterator implements Iterator<Write> {

        /**
         * Current {@link Segment} {@link Segment#writes() write} iterator.
         */
        private Iterator<Write> it;

        /**
         * The next {@link Write} to return from {@link #next()}.
         */
        private Write next;

        /**
         * Iterator over a snapshot of the {@link #segments}.
         */
        private final Iterator<Segment> segIt;

        /**
         * Construct a new instance.
         */
        private AcceptedWriteIterator() {
            segIt = new ArrayList<>(segments).iterator();
            it = null;
            next = findNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Write next() {
            Write current = next;
            if(current != null) {
                next = findNext();
                return current;
            }
            else {
                throw new NoSuchElementException();
            }
        }

        /**
         * Flip to the next {@link Segment} iterator.
         */
        private Write findNext() {
            if(it != null && it.hasNext()) {
                return it.next();
            }
            else if(segIt.hasNext()) { // flip
                it = segIt.next().writes().iterator();
                return findNext();
            }
            else {
                return null;
            }
        }

    }

    /**
     * View into the {@link Memory} of the {@link Database}.
     *
     * @author Jeff Nelson
     */
    private class CacheState implements Memory {

        private CacheState() {/* singleton */}

        @Override
        public boolean contains(long record) {
            Composite composite = Composite.create(Identifier.of(record));
            return tableCache.getIfPresent(composite) != null;
        }

        @Override
        public boolean contains(String key) {
            Composite composite = Composite.create(Text.wrapCached(key));
            return indexCache.getIfPresent(composite) != null;
        }

        @Override
        public boolean contains(String key, long record) {
            Composite composite = Composite.create(Identifier.of(record),
                    Text.wrapCached(key));
            return tablePartialCache.getIfPresent(composite) != null
                    || contains(record);
        }

    }

    /**
     * Utilities for processing, handling and transforming data within a
     * {@link Database}.
     *
     * @author Jeff Nelson
     */
    private static class Internals {

        /**
         * Transform the keys and values in a {@link Map} that is already
         * assumed to be in sorted order according on the provided
         * {@code comparator} and would continue to be in the same sorted order
         * after applying the {@code keyTransformer} (e.g., one that comes from
         * an {@link IndexRecord}).
         * 
         * @param data
         * @param keyTransformer
         * @param valueTransformer
         * @param comparator
         * @return a {@link Map} that features the data after applying the
         *         {@code keyTransformer} and {@code valueTransformer}. The
         *         entries in the {@link Map} retain the original assumed sorted
         *         order of the pre-transformed {@code data}
         */
        public static <K, K2, V, V2> Map<K2, Set<V2>> transformAssumedSortedMap(
                Map<K, Set<V>> data, Function<K, K2> keyTransformer,
                Function<V, V2> valueTransformer, Comparator<K2> comparator) {
            Map<K2, Set<V2>> transformed = new LinkedHashMap<>(data.size());
            for (Entry<K, Set<V>> entry : data.entrySet()) {
                K2 key = keyTransformer.apply(entry.getKey());
                Set<V2> values = new LinkedHashSet<>(entry.getValue().size());
                for (V value : entry.getValue()) {
                    values.add(valueTransformer.apply(value));
                }
                transformed.put(key, values);
            }
            transformed = new BridgeSortMap<>(transformed, comparator);
            return transformed;
        }
    }

    /**
     * Dynamic runtime configuration options.
     *
     * @author Jeff Nelson
     */
    private final class Options {

        /**
         * Return {@code true} if {@link CorpusRecord corpus records} should be
         * read from disk asynchronously.
         * 
         * @return the optional configuration value.
         */
        boolean enableAsyncCorpusDataReads() {
            return running && ENABLE_ASYNC_DATA_READS;
        }

        /**
         * Return {@code true} if {@link IndexRecord index records} should be
         * read from disk asynchronously.
         * 
         * @return the optional configuration value.
         */
        boolean enableAsyncIndexDataReads() {
            return running && ENABLE_ASYNC_DATA_READS;
        }

        /**
         * Return {@code true} if {@link TableRecord table records} should be
         * read from disk asynchronously.
         * 
         * @return the optional configuration value.
         */
        boolean enableAsyncTableDataReads() {
            return running && ENABLE_ASYNC_DATA_READS;
        }
    }

    /**
     * {@link Cache} wrapper that is aware of whether the {@link Database} is
     * running and behaves accordingly.
     *
     * @author Jeff Nelson
     */
    private class RunningAwareCache<K, V> implements LoadingCache<K, V> {

        /**
         * The underlying {@link Cache}.
         */
        private final LoadingCache<K, V> cache;

        /**
         * The loading function.
         */
        private final Function<K, V> loader;

        /**
         * Construct a new instance.
         * 
         * @param cache
         */
        RunningAwareCache(LoadingCache<K, V> cache, Function<K, V> loader) {
            this.cache = cache;
            this.loader = loader;
        }

        @Override
        @Deprecated
        public V apply(K key) {
            return running ? cache.apply(key) : null;
        }

        @Override
        public ConcurrentMap<K, V> asMap() {
            return running ? cache.asMap() : Maps.newConcurrentMap();
        }

        @Override
        public void cleanUp() {
            if(running) {
                cache.cleanUp();
            }
        }

        @Override
        public V get(K key) throws ExecutionException {
            return running ? cache.get(key) : loader.apply(key);
        }

        @Override
        public V get(K key, Callable<? extends V> loader)
                throws ExecutionException {
            try {
                return running ? cache.get(key, loader) : loader.call();
            }
            catch (Exception e) {
                throw new ExecutionException(e.getMessage(), e);
            }
        }

        @Override
        public ImmutableMap<K, V> getAll(Iterable<? extends K> keys)
                throws ExecutionException {
            return running ? cache.getAll(keys) : ImmutableMap.of();
        }

        @Override
        public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
            return running ? cache.getAllPresent(keys) : ImmutableMap.of();
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable V getIfPresent(
                Object key) {
            return running ? cache.getIfPresent(key) : null;
        }

        @Override
        public V getUnchecked(K key) {
            return running ? cache.getUnchecked(key) : null;
        }

        @Override
        public void invalidate(Object key) {
            if(running) {
                cache.invalidate(key);
            }
        }

        @Override
        public void invalidateAll() {
            if(running) {
                cache.invalidateAll();
            }
        }

        @Override
        public void invalidateAll(Iterable<?> keys) {
            if(running) {
                cache.invalidateAll(keys);
            }
        }

        @Override
        public void put(K key, V value) {
            if(running) {
                cache.put(key, value);
            }
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            if(running) {
                cache.putAll(m);
            }
        }

        @Override
        public void refresh(K key) {
            if(running) {
                cache.refresh(key);
            }
        }

        @Override
        public long size() {
            return running ? cache.size() : 0;
        }

        @Override
        public CacheStats stats() {
            return running ? cache.stats() : new CacheStats(0, 0, 0, 0, 0, 0);
        }

    }

    /**
     * The {@link SegmentStorageSystem} for a {@link Database}.
     *
     * @author Jeff Nelson
     */
    private static class Storage implements SegmentStorageSystem {

        private static String FILESYSTEM_HOOK_FILE_NAME = ".fs";

        /**
         * The directory where .{@link Segment seg} files are stored.
         */
        private final Path directory;

        /**
         * Used to hook into disk space APIs needed for conformity with
         * {@link SegmentStorageSystem} interface.
         */
        private final transient File fs;

        /**
         * Controls concurrent access to modify the {@link #segments()}.
         */
        private final Lock lock;

        /**
         * A live collection of the {@link Segments} in the {@link Database}.
         */
        private final List<Segment> segments;

        /**
         * Construct a new instance.
         * 
         * @param directory
         * @param segments
         */
        private Storage(Path directory, List<Segment> segments, Lock lock) {
            this.directory = directory;
            FileSystem.mkdirs(directory);
            this.segments = segments;
            this.lock = lock;
            this.fs = directory.resolve(FILESYSTEM_HOOK_FILE_NAME).toFile();
            try {
                fs.createNewFile(); // File must "exist" in order to
                                    // hook into disk space APIs
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }

        @Override
        public long availableDiskSpace() {
            return fs.getUsableSpace();
        }

        /**
         * Return the full {@link Path} for the directory where the
         * {@link #segments segment} files are stored.
         * 
         * @return the storage directory
         */
        @SuppressWarnings("unused")
        public Path directory() {
            return directory;
        }

        /**
         * Return a {@link Stream} of all the storage files.
         * 
         * @return the storage files
         */
        public Stream<Path> files() {
            return FileSystem.ls(directory).filter(file -> !file.getFileName()
                    .toString().equals(FILESYSTEM_HOOK_FILE_NAME));
        }

        @Override
        public Lock lock() {
            return lock;
        }

        @Override
        public Path save(Segment segment) {
            Path file = directory.resolve(UUID.randomUUID() + ".seg");
            segment.transfer(file);
            return file;
        }

        @Override
        public List<Segment> segments() {
            return segments;
        }

        @Override
        public long totalDiskSpace() {
            return fs.getTotalSpace();
        }
    }

}
