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
package com.cinchapi.concourse.server.storage.db;

import static com.cinchapi.concourse.server.GlobalState.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.collect.concurrent.ThreadFactories;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.TObjectSorter;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.BaseStore;
import com.cinchapi.concourse.server.storage.Memory;
import com.cinchapi.concourse.server.storage.PermanentStore;
import com.cinchapi.concourse.server.storage.cache.NoOpCache;
import com.cinchapi.concourse.server.storage.db.kernel.CorpusArtifact;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.db.kernel.Segment.Receipt;
import com.cinchapi.concourse.server.storage.db.kernel.SegmentLoadingException;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Comparators;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.ReadOnlyIterator;
import com.cinchapi.concourse.util.TStrings;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * The {@link Database} is the {@link PermanentStore} for data. The
 * Database accepts {@link Write} objects that are initially stored in a
 * {@link Buffer} and converts them {@link Revision Revisions} that are stored
 * within distinct {@link Segment Segments}. Each {@link Segment} is broken up
 * into {@link Chunk Chunks} that provided optimized read-views.
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
public final class Database extends BaseStore implements PermanentStore {

    /**
     * Return an {@link Iterator} that will iterate over all of the
     * {@link TableRevision PrimaryRevisions} that are stored in the
     * {@code dbStore}. The iterator streams the revisions directly from disk
     * using a buffer size that is equal to {@link GlobalState#BUFFER_PAGE_SIZE}
     * so it should have a predictable memory footprint.
     * 
     * @param dbStore
     * @return the iterator
     */
    public static Iterator<Revision<PrimaryKey, Text, Value>> onDiskStreamingIterator(
            final String dbStore) {
        return new ReadOnlyIterator<Revision<PrimaryKey, Text, Value>>() {

            private final String directory = FileSystem.makePath(dbStore,
                    SEGMENTS_SUBDIRECTORY);
            private final Iterator<String> fileIt = FileSystem
                    .fileOnlyIterator(directory);
            private Iterator<Revision<PrimaryKey, Text, Value>> it = null;
            {
                flip();
            }

            @Override
            public boolean hasNext() {
                if(it == null) {
                    return false;
                }
                else if(!it.hasNext() && fileIt.hasNext()) {
                    flip();
                    return hasNext();
                }
                else if(!it.hasNext()) {
                    return false;
                }
                else {
                    return true;
                }
            }

            @Override
            public Revision<PrimaryKey, Text, Value> next() {
                if(hasNext()) {
                    return it.next();
                }
                else {
                    return null;
                }
            }

            private void flip() {
                if(fileIt.hasNext()) {
                    Path file = Paths.get(fileIt.next());
                    try {
                        it = Segment.load(file).table().iterator();
                    }
                    catch (SegmentLoadingException e) {
                        flip();
                    }
                }
            }

        };
    }

    /**
     * Return a cache for records of type {@code T}.
     * 
     * @return the cache
     */
    private <T> Cache<Composite, T> buildCache() {
        Cache<Composite, T> cache = CacheBuilder.newBuilder()
                .maximumSize(100000).softValues().build();
        return new RunningAwareCache<>(cache);
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
     * The subdirectory of {@link #directory} where the {@link Segment} files
     * are stored.
     */
    private static final String SEGMENTS_SUBDIRECTORY = "segments";

    /**
     * Global flag that indicates if search data is cached.
     */
    // Copied here as a final variable for (hopeful) performance gains.
    private static final boolean ENABLE_SEARCH_CACHE = GlobalState.ENABLE_SEARCH_CACHE;

    /**
     * The full {@link Path} for the directory where the {@link #segments
     * segment} files are stored.
     */
    private final transient Path $segments;

    /**
     * A flag to indicate if the Database has verified the data it is seeing is
     * acceptable. We use this flag to handle the case where the server
     * unexpectedly crashes before removing a Buffer page and tries to transport
     * Writes that have already been accepted. The SLA for this flag is that the
     * Database will assume no Writes are acceptable (and will therefore
     * manually verify) until it sees one, at which point it will assume all
     * subsequent Writes are acceptable.
     */
    private transient boolean acceptable = false;

    /*
     * RECORD CACHES
     * -------------
     * Records are cached in memory to reduce the number of seeks required. When
     * writing new revisions, we check the appropriate caches for relevant
     * records and append the new revision so that the cached data doesn't grow
     * stale.
     * 
     * The caches are only populated if the Database is #running (see
     * #accept(Write)). Attempts to get a Record when the Database is not
     * running will ignore the cache by virtue of an internal wrapper that has
     * the appropriate detection.
     */
    private final Cache<Composite, TableRecord> tableCache = buildCache();
    private final Cache<Composite, TableRecord> tablePartialCache = buildCache();
    private final Cache<Composite, IndexRecord> indexCache = buildCache();
    private final Cache<Composite, CorpusRecord> corpusCache = ENABLE_SEARCH_CACHE
            ? buildCache()
            : new NoOpCache<>();

    /**
     * The location where the Database stores data.
     */
    private final transient Path directory;

    /**
     * Lock used to ensure the object is ThreadSafe. This lock provides access
     * to a masterLock.readLock()() and masterLock.writeLock()().
     */
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
    private final transient List<Segment> segments = Lists.newArrayList();

    /**
     * An {@link ExecutorService} that is passed to {@link #seg0} to handle
     * writing tasks asynchronously in the background.
     */
    private transient AwaitableExecutorService writer;

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
        this.directory = directory;
        this.$segments = directory.resolve(SEGMENTS_SUBDIRECTORY);
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

    @Override
    public void accept(Write write) {
        // CON-83: Keeping manually verifying writes until we find one that is
        // acceptable, after which assume all subsequent writes are acceptable.
        if(!acceptable && ((write.getType() == Action.ADD
                && !verify(write.getKey().toString(),
                        write.getValue().getTObject(),
                        write.getRecord().longValue()))
                || (write.getType() == Action.REMOVE
                        && verify(write.getKey().toString(),
                                write.getValue().getTObject(),
                                write.getRecord().longValue())))) {
            acceptable = true;
        }
        if(acceptable) {
            // NOTE: This approach is thread safe because write locking happens
            // in each of #seg0's individual Blocks, and furthermore this method
            // is only called from the Buffer, which transports data serially.
            if(running) {
                try {
                    Receipt receipt = seg0.acquire(write, writer);
                    Logger.debug("Indexed '{}' in {}", write, seg0);

                    // Updated cached records
                    TableRecord cpr = tableCache.getIfPresent(
                            receipt.table().getLocatorComposite());
                    TableRecord cppr = tablePartialCache.getIfPresent(
                            receipt.table().getLocatorKeyComposite());
                    IndexRecord csr = indexCache.getIfPresent(
                            receipt.index().getLocatorComposite());
                    if(cpr != null) {
                        cpr.append(receipt.table().revision());
                    }
                    if(cppr != null) {
                        cppr.append(receipt.table().revision());
                    }
                    if(csr != null) {
                        csr.append(receipt.index().revision());
                    }
                    if(ENABLE_SEARCH_CACHE) {
                        for (CorpusArtifact artifact : receipt.corpus()) {
                            CorpusRecord ccr = corpusCache.getIfPresent(
                                    artifact.getLocatorKeyComposite());
                            if(ccr != null) {
                                ccr.append(artifact.revision());
                            }
                        }
                    }
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
        else {
            Logger.warn("The Engine refused to accept {} because "
                    + "it appears that the data was already transported. "
                    + "This indicates that the server shutdown prematurely.",
                    write);
        }
    }

    @Override
    public void accept(Write write, boolean sync) {
        // NOTE: The functionality to optionally sync when accepting writes is
        // not really supported in the Database, but is implemented to conform
        // with the PermanentStore interface.
        accept(write);
        if(sync) {
            sync();
        }
    }

    @Override
    public Map<Long, String> audit(long record) {
        PrimaryKey L = PrimaryKey.wrap(record);
        TableRecord table = getTableRecord(L);
        return table.audit();
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        PrimaryKey L = PrimaryKey.wrap(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L, K);
        return table.audit(K);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Map<Value, Set<PrimaryKey>> data = index.get();
        return Transformers.transformTreeMapSet(data, Value::getTObject,
                PrimaryKey::longValue, TObjectSorter.INSTANCE);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Map<Value, Set<PrimaryKey>> data = index.get(timestamp);
        return Transformers.transformTreeMapSet(data, Value::getTObject,
                PrimaryKey::longValue, TObjectSorter.INSTANCE);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        PrimaryKey L = PrimaryKey.wrap(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L);
        Map<Long, Set<Value>> data = table.chronologize(K, start, end);
        return Transformers.transformMapSet(data, Functions.identity(),
                Value::getTObject);
    }

    @Override
    public boolean contains(long record) {
        PrimaryKey L = PrimaryKey.wrap(record);
        TableRecord table = getTableRecord(L);
        return !table.isEmpty();
    }

    @Override
    public Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Value[] Ks = Transformers.transformArray(values, Value::wrap,
                Value.class);
        Map<PrimaryKey, Set<Value>> map = index.findAndGet(timestamp, operator,
                Ks);
        return Transformers.transformTreeMapSet(map, PrimaryKey::longValue,
                Value::getTObject, Long::compare);
    }

    @Override
    public Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        Text L = Text.wrapCached(key);
        IndexRecord index = getIndexRecord(L);
        Value[] Ks = Transformers.transformArray(values, Value::wrap,
                Value.class);
        Map<PrimaryKey, Set<Value>> map = index.findAndGet(operator, Ks);
        return Transformers.transformTreeMapSet(map, PrimaryKey::longValue,
                Value::getTObject, Long::compare);
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
    public Set<TObject> gather(String key, long record) {
        Text L = Text.wrapCached(key);
        PrimaryKey V = PrimaryKey.wrap(record);
        IndexRecord index = getIndexRecord(L);
        Set<Value> Ks = index.gather(V);
        return Transformers.transformSet(Ks, Value::getTObject);
    }

    @Override
    public Set<TObject> gather(String key, long record, long timestamp) {
        Text L = Text.wrapCached(key);
        PrimaryKey V = PrimaryKey.wrap(record);
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

    @Override
    public Memory memory() {
        Verify.that(running,
                "Cannot return the memory of a stopped Database instance");
        return memory;
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
            Multimap<PrimaryKey, Integer> reference = HashMultimap.create();
            boolean initial = true;
            int offset = 0;
            for (String word : words) {
                Multimap<PrimaryKey, Integer> temp = HashMultimap.create();
                if(GlobalState.STOPWORDS.contains(word)) {
                    // When skipping a stop word, we must record an offset to
                    // correctly determine if the next term match is in the
                    // correct relative position to the previous term match
                    ++offset;
                    continue;
                }
                Text K = Text.wrap(word);
                CorpusRecord corpus = getCorpusRecord(L, K);
                Set<Position> positions = corpus.get(K);
                for (Position position : positions) {
                    PrimaryKey record = position.getPrimaryKey();
                    int pos = position.getIndex();
                    if(initial) {
                        temp.put(record, pos);
                    }
                    else {
                        for (int current : reference.get(record)) {
                            if(pos == current + 1 + offset) {
                                temp.put(record, pos);
                            }
                        }
                    }
                }
                initial = false;
                reference = temp;
                offset = 0;
            }

            // Result Scoring: Scoring is simply the number of times the query
            // appears in a Record [e.g. the number of Positions mapped from
            // key: #reference.get(key).size()]. The total number of positions
            // in #reference is equal to the total number of times a document
            // appears in the corpus [e.g. reference.asMap().values().size()].
            Multimap<Integer, PrimaryKey> sorted = TreeMultimap.create(
                    Collections.<Integer> reverseOrder(),
                    PrimaryKey.Sorter.INSTANCE);
            for (Entry<PrimaryKey, Collection<Integer>> entry : reference
                    .asMap().entrySet()) {
                sorted.put(entry.getValue().size(), entry.getKey());
            }
            Set<Long> results = sorted.values().stream()
                    .map(PrimaryKey::longValue)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return results;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        PrimaryKey L = PrimaryKey.wrap(record);
        TableRecord table = getTableRecord(L);
        Map<Text, Set<Value>> data = table.get();
        return Transformers.transformTreeMapSet(data, Text::toString,
                Value::getTObject,
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        PrimaryKey L = PrimaryKey.wrap(record);
        TableRecord table = getTableRecord(L);
        Map<Text, Set<Value>> data = table.get(timestamp);
        return Transformers.transformTreeMapSet(data, Text::toString,
                Value::getTObject,
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        PrimaryKey L = PrimaryKey.wrap(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L, K);
        Set<Value> data = table.get(K);
        return Transformers.transformSet(data, Value::getTObject);
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp) {
        PrimaryKey L = PrimaryKey.wrap(record);
        Text K = Text.wrapCached(key);
        TableRecord table = getTableRecord(L, K);
        Set<Value> data = table.get(K, timestamp);
        return Transformers.transformSet(data, Value::getTObject);
    }

    @Override
    public void start() {
        if(!running) {
            Logger.info("Database configured to store data in {}", directory);
            running = true;
            this.writer = new AwaitableExecutorService(
                    Executors.newCachedThreadPool(ThreadFactories
                            .namingThreadFactory("DatabaseWriter")));
            this.segments.clear();
            ArrayBuilder<Runnable> tasks = ArrayBuilder.builder();
            FileSystem.mkdirs($segments);
            List<Segment> segments = Collections
                    .synchronizedList(this.segments);
            FileSystem.ls($segments).forEach(file -> tasks.add(() -> {
                try {
                    Segment segment = Segment.load(file);
                    segments.add(segment);
                }
                catch (SegmentLoadingException e) {
                    Logger.error("Error when trying to load Segment {}", file);
                    Logger.error("", e);
                }
            }));
            if(tasks.length() > 0) {
                AwaitableExecutorService loader = new AwaitableExecutorService(
                        Executors.newCachedThreadPool(ThreadFactories
                                .namingThreadFactory("DatabaseLoader")));
                try {
                    loader.await((task, error) -> Logger.error(
                            "Unexpected error when trying to load Database Segmens: {}",
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
                    if(current.equals(previous)) {
                        lit.previous();
                        lit.previous();
                        lit.remove();
                        Logger.warn(
                                "Segment {} was not loaded because it contains duplicate data. It has been scheduled for garbage collection.",
                                previous);
                        lit.next();
                        // TODO: mark #previous for garbage collection
                    }
                }
                else {
                    lit.next();
                }
            }

            triggerSync(false);
            memory = new CacheState();
        }

    }

    @Override
    public void stop() {
        if(running) {
            running = false;
            writer.shutdown();
            memory = null;
            for (Cache<Composite, ?> cache : ImmutableList.of(tableCache,
                    tablePartialCache, indexCache, corpusCache)) {
                cache.invalidateAll();
            }
        }
    }

    @Override
    public void sync() {
        triggerSync();
    }

    /**
     * Create new blocks and sync the current blocks to disk.
     */
    @GuardedBy("triggerSync(boolean)")
    public void triggerSync() {
        triggerSync(true);
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        PrimaryKey L = PrimaryKey.wrap(record);
        Text K = Text.wrapCached(key);
        Value V = Value.wrap(value);
        TableRecord table = getTableRecord(L, K);
        return table.contains(K, V);
    }

    @Override
    public boolean verify(String key, TObject value, long record,
            long timestamp) {
        PrimaryKey L = PrimaryKey.wrap(record);
        Text K = Text.wrapCached(key);
        Value V = Value.wrap(value);
        TableRecord table = getTableRecord(L, K);
        return table.contains(K, V, timestamp);
    }

    /**
     * Return the TableRecord identifier by {@code primaryKey}.
     * 
     * @param primaryKey
     * @return the TableRecord
     */
    private TableRecord getTableRecord(PrimaryKey primaryKey) {
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(primaryKey);
            return tableCache.get(composite, () -> {
                TableRecord $ = TableRecord.create(primaryKey);
                for (Segment segment : segments) {
                    segment.table().seek(composite, $);
                }
                return $;
            });
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
     * {@code primaryKey}.
     * <p>
     * While the returned {@link TableRecord} may not be
     * {@link TableRecord#isPartial() partial}, the caller should interact
     * with it as if it is (e.g. do not perform reads for any other keys besides
     * {@code key}.
     * </p>
     * 
     * @param primaryKey
     * @param key
     * @return the TableRecord
     */
    private TableRecord getTableRecord(PrimaryKey primaryKey, Text key) {
        masterLock.readLock().lock();
        try {
            // Before loading a partial record, see if the full record is
            // present in memory.
            TableRecord table = tableCache
                    .getIfPresent(Composite.create(primaryKey));
            if(table == null) {
                Composite composite = Composite.create(primaryKey, key);
                table = tablePartialCache.get(composite, () -> {
                    TableRecord $ = TableRecord.createPartial(primaryKey, key);
                    for (Segment segment : segments) {
                        segment.table().seek(composite, $);
                    }
                    return $;
                });
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
     * Return the CorpusRecord identified by {@code key}.
     * 
     * @param key
     * @param query
     * @param toks {@code query} split by whitespace
     * @return the CorpusRecord
     */
    private CorpusRecord getCorpusRecord(Text key, Text infix) {
        // NOTE: We do not cache CorpusRecords because they have the potential
        // to be VERY large. Holding references to them in a cache would prevent
        // them from being garbage collected resulting in more OOMs.
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(key, infix);
            return corpusCache.get(composite, () -> {
                CorpusRecord $ = CorpusRecord.createPartial(key, infix);
                for (Segment segment : segments) {
                    segment.corpus().seek(composite, $);
                }
                return $;
            });
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
            return indexCache.get(composite, () -> {
                IndexRecord $ = IndexRecord.create(key);
                for (Segment segment : segments) {
                    segment.index().seek(composite, $);
                }
                return $;
            });
        }
        catch (ExecutionException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Create new mutable blocks and sync the current blocks to disk if
     * {@code doSync} is {@code true}.
     * 
     * @param flush - a flag that controls whether we actually perform a
     *            sync or not. Sometimes this method is called when there is no
     *            data to sync and we just want to create new blocks (e.g. on
     *            initial startup).
     */
    private void triggerSync(boolean flush) {
        masterLock.writeLock().lock();
        try {
            if(flush) {
                Path file = $segments.resolve(UUID.randomUUID() + ".seg");
                String id = seg0.id();
                seg0.transfer(file);
                Logger.debug("Completed sync of {} to disk at {}", id, file);
            }
            segments.add((seg0 = Segment.create()));
        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    /**
     * {@link Cache} wrapper that is aware of whether the {@link Database} is
     * running and behaves accordingly.
     *
     * @author Jeff Nelson
     */
    private class RunningAwareCache<K, V> implements Cache<K, V> {

        /**
         * The underlying {@link Cache}.
         */
        private final Cache<K, V> cache;

        /**
         * Construct a new instance.
         * 
         * @param cache
         */
        RunningAwareCache(Cache<K, V> cache) {
            this.cache = cache;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable V getIfPresent(
                Object key) {
            return running ? cache.getIfPresent(key) : null;
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
        public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
            return running ? cache.getAllPresent(keys) : ImmutableMap.of();
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
        public void invalidate(Object key) {
            if(running) {
                cache.invalidate(key);
            }
        }

        @Override
        public void invalidateAll(Iterable<?> keys) {
            if(running) {
                cache.invalidateAll(keys);
            }
        }

        @Override
        public void invalidateAll() {
            if(running) {
                cache.invalidateAll();
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
            Composite composite = Composite.create(PrimaryKey.wrap(record));
            return tableCache.getIfPresent(composite) != null;
        }

        @Override
        public boolean contains(String key) {
            Composite composite = Composite.create(Text.wrapCached(key));
            return indexCache.getIfPresent(composite) != null;
        }

        @Override
        public boolean contains(String key, long record) {
            Composite composite = Composite.create(PrimaryKey.wrap(record),
                    Text.wrapCached(key));
            return tablePartialCache.getIfPresent(composite) != null
                    || contains(record);
        }

    }

}
