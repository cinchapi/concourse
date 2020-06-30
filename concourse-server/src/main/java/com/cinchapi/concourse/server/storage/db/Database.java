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
package com.cinchapi.concourse.server.storage.db;

import static com.cinchapi.concourse.server.GlobalState.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.collect.concurrent.ThreadFactories;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.TObjectSorter;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.BaseStore;
import com.cinchapi.concourse.server.storage.Memory;
import com.cinchapi.concourse.server.storage.PermanentStore;
import com.cinchapi.concourse.server.storage.db.Segment.Receipt;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Comparators;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.ReadOnlyIterator;
import com.cinchapi.concourse.util.TStrings;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

/**
 * The {@code Database} is the {@link PermanentStore} for data. The
 * Database accepts {@link Write} objects that are initially stored in a
 * {@link Buffer} and converts them to {@link Revision} objects that are stored
 * in various {@link Block} objects, which provide indexed views for optimized
 * reads.
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class Database extends BaseStore implements PermanentStore {

    /**
     * Return an {@link Iterator} that will iterate over all of the
     * {@link PrimaryRevision PrimaryRevisions} that are stored in the
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

            private final String backingStore = FileSystem.makePath(dbStore,
                    Segment.PRIMARY_BLOCK_DIRECTORY);
            private final Iterator<String> fileIt = FileSystem
                    .fileOnlyIterator(backingStore);
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
                    String file = fileIt.next();
                    if(file.endsWith(Block.BLOCK_NAME_EXTENSION)) {
                        String id = Block.getId(file);
                        it = new PrimaryBlock(id, backingStore, true)
                                .iterator(); /* authorized */
                    }
                    else {
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
    private static <T> Cache<Composite, T> buildCache() {
        return CacheBuilder.newBuilder().maximumSize(100000).softValues()
                .build();
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

    /*
     * BLOCK COLLECTIONS
     * -----------------
     * We maintain a view-collection to all the blocks, in chronological order
     * for backwards compatibility of upgrade tasks.
     */
    // @formatter:off
    protected final transient List<PrimaryBlock> cpb = new BlockView<>(
            seg -> seg.primary());

    protected final transient List<SecondaryBlock> csb = new BlockView<>(
            seg -> seg.secondary());

    protected final transient List<SearchBlock> ctb = new BlockView<>(
            seg -> seg.search());
    // @formatter:on

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

    /**
     * The location where the Database stores data.
     */
    private final transient String backingStore;

    /*
     * RECORD CACHES
     * -------------
     * Records are cached in memory to reduce the number of seeks required. When
     * writing new revisions, we check the appropriate caches for relevant
     * records and append the new revision so that the cached data doesn't grow
     * stale.
     */
    private final Cache<Composite, PrimaryRecord> cpc = buildCache();
    private final Cache<Composite, PrimaryRecord> cppc = buildCache();
    private final Cache<Composite, SecondaryRecord> csc = buildCache();

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
     * An {@link ExecutorService} that handles asynchronous reading tasks in the
     * background.
     */
    private transient AwaitableExecutorService reader;

    /**
     * A flag to indicate if the Buffer is running or not.
     */
    private transient boolean running = false;

    /**
     * CURRENT SEGMENT POINTER
     * ----------------------
     * We hold direct references to the current Segment. This pointer changes
     * whenever the database triggers a sync operation.
     */
    private transient Segment seg0;
    /**
     * SEGMENT COLLECTION
     * -----------------
     * We maintain a collection of all the segments, in chronological order, so
     * that we can seek for the necessary revisions and populate a requested
     * record.
     */
    private final transient SortedSet<Segment> segments = Sets.newTreeSet();
    /**
     * An {@link ExecutorService} that handles asynchronous writing tasks in the
     * background.
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
     * @param backingStore
     */
    public Database(String backingStore) {
        this.backingStore = backingStore;
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
                    Receipt receipt = seg0.transfer(write, writer);

                    // Updated cached records
                    PrimaryRecord cpr = cpc
                            .getIfPresent(Composite.create(write.getRecord()));
                    PrimaryRecord cppr = cppc.getIfPresent(Composite
                            .create(write.getRecord(), write.getKey()));
                    SecondaryRecord csr = csc
                            .getIfPresent(Composite.create(write.getKey()));
                    if(cpr != null) {
                        cpr.append(receipt.primaryRevision());
                    }
                    if(cppr != null) {
                        cppr.append(receipt.primaryRevision());
                    }
                    if(csr != null) {
                        csr.append(receipt.secondaryRevision());
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
                seg0.transfer(write);
            }
        }
        else {
            Logger.warn(
                    "The Engine refused to accept {} because "
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
        return getPrimaryRecord(PrimaryKey.wrap(record)).audit();
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        Text key0 = Text.wrapCached(key);
        return getPrimaryRecord(PrimaryKey.wrap(record), key0).audit(key0);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        return Transformers.transformTreeMapSet(
                getSecondaryRecord(Text.wrapCached(key)).browse(),
                Value::getTObject, PrimaryKey::longValue,
                TObjectSorter.INSTANCE);
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        return Transformers.transformTreeMapSet(
                getSecondaryRecord(Text.wrapCached(key)).browse(timestamp),
                Value::getTObject, PrimaryKey::longValue,
                TObjectSorter.INSTANCE);
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        return Transformers.transformMapSet(
                getPrimaryRecord(PrimaryKey.wrap(record))
                        .chronologize(Text.wrapCached(key), start, end),
                com.google.common.base.Functions.<Long> identity(),
                Value::getTObject);
    }

    @Override
    public boolean contains(long record) {
        return !getPrimaryRecord(PrimaryKey.wrap(record)).isEmpty();
    }

    @Override
    public Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        SecondaryRecord record = getSecondaryRecord(Text.wrapCached(key));
        Map<PrimaryKey, Set<Value>> map = record.explore(timestamp, operator,
                Transformers.transformArray(values, Value::wrap, Value.class));
        return Transformers.transformTreeMapSet(map, PrimaryKey::longValue,
                Value::getTObject, Long::compare);
    }

    @Override
    public Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        SecondaryRecord record = getSecondaryRecord(Text.wrapCached(key));
        Map<PrimaryKey, Set<Value>> map = record.explore(operator,
                Transformers.transformArray(values, Value::wrap, Value.class));
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
                "Insufficient number of blocks identified by %s", id);
        StringBuilder sb = new StringBuilder();
        sb.append(segment.primary().dump());
        sb.append(segment.secondary().dump());
        if(segment.search() != null) {
            sb.append(segment.search().dump());
        }
        return sb.toString();
    }

    @Override
    public Set<TObject> gather(String key, long record) {
        SecondaryRecord index = getSecondaryRecord(Text.wrapCached(key));
        Set<Value> values = index.gather(PrimaryKey.wrap(record));
        return Transformers.transformSet(values, Value::getTObject);
    }

    @Override
    public Set<TObject> gather(String key, long record, long timestamp) {
        SecondaryRecord index = getSecondaryRecord(Text.wrapCached(key));
        Set<Value> values = index.gather(PrimaryKey.wrap(record), timestamp);
        return Transformers.transformSet(values, Value::getTObject);
    }

    /**
     * Return the location where the Database stores its data.
     * 
     * @return the backingStore
     */
    @Restricted
    public String getBackingStore() {
        return backingStore;
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
        return Transformers
                .transformSet(
                        getSearchRecord(Text.wrapCached(key), Text.wrap(query))
                                .search(Text.wrap(query)),
                        PrimaryKey::longValue);
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        return Transformers.transformTreeMapSet(
                getPrimaryRecord(PrimaryKey.wrap(record)).browse(),
                Text::toString, Value::getTObject,
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        return Transformers.transformTreeMapSet(
                getPrimaryRecord(PrimaryKey.wrap(record)).browse(timestamp),
                Text::toString, Value::getTObject,
                Comparators.CASE_INSENSITIVE_STRING_COMPARATOR);
    }

    @Override
    public Set<TObject> select(String key, long record) {
        Text key0 = Text.wrapCached(key);
        return Transformers.transformSet(
                getPrimaryRecord(PrimaryKey.wrap(record), key0).fetch(key0),
                Value::getTObject);
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp) {
        Text key0 = Text.wrapCached(key);
        return Transformers
                .transformSet(getPrimaryRecord(PrimaryKey.wrap(record), key0)
                        .fetch(key0, timestamp), Value::getTObject);
    }

    @Override
    public void start() {
        if(!running) {
            running = true;
            Logger.info("Database configured to store data in {}",
                    backingStore);
            this.writer = new AwaitableExecutorService(
                    Executors.newCachedThreadPool(ThreadFactories
                            .namingThreadFactory("database-write-thread")));
            this.reader = new AwaitableExecutorService(
                    Executors.newCachedThreadPool(ThreadFactories
                            .namingThreadFactory("Storage Block Loader")));
            segments.clear();
            Path directory = Paths.get(backingStore);
            ArrayBuilder<Runnable> tasks = ArrayBuilder.builder();
            Path cpb = Paths.get(backingStore)
                    .resolve(Segment.PRIMARY_BLOCK_DIRECTORY);
            FileSystem.mkdirs(cpb.toString());
            FileSystem.ls(cpb)
                    .filter(file -> file.toString()
                            .endsWith(Block.BLOCK_NAME_EXTENSION))
                    .map(Path::toFile).filter(file -> file.length() > 0)
                    .map(file -> file.getName()
                            .split(Block.BLOCK_NAME_EXTENSION)[0])
                    .forEach(id -> tasks.add(() -> {
                        try {
                            Segment segment = Segment.load(id, directory);
                            segments.add(segment);
                        }
                        catch (MalformedBlockException e) {
                            Logger.warn(
                                    "{}. As a result the Block was NOT loaded. A malformed block is usually an indication that the Block was only partially synced to disk before Concourse Server shutdown. In this case, it is safe to delete any Block files that were written for id {}",
                                    e.getMessage(), id);
                        }
                        catch (SegmentLoadingException e) {
                            Logger.error(
                                    "An error occured while loading {} metadata for {}",
                                    e.blockType(), id);
                            Logger.error("", e.error());
                        }
                        catch (IllegalStateException e) {
                            Logger.error(
                                    "A storage segment failed to validate while loading {}",
                                    id);
                            Logger.error("", e);
                        }
                    }));
            if(tasks.length() > 0) {
                try {
                    reader.await((task, error) -> Logger.error(
                            "Unexpected error when trying to load Blocks: {}",
                            error), tasks.build());
                }
                catch (InterruptedException e) {
                    Logger.error(
                            "The Database was interrupted while starting...",
                            e);
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            // Remove duplicate Segments. Segment duplication can occur when the
            // server crashes and a Segment is only partially synced (e.g. the
            // primary and secondary Block are written but the Search Block is
            // not). When the server restarts, it will try to sync the Segment
            // again, generating duplicate Blocks on disk for the Blocks that
            // succeeded in syncing before the crash
            Iterator<Segment> it = segments.iterator();
            Set<String> checksums = Sets
                    .newHashSetWithExpectedSize(segments.size());
            while (it.hasNext()) {
                Segment segment = it.next();
                if(!checksums.add(segment.checksum())) {
                    it.remove();
                    Logger.warn(
                            "Segment {} contains duplicate data, so it was not loaded. You can safely delete all of it's files.",
                            segment.id());
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
            reader.shutdown();
            writer.shutdown();
            memory = null;
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
        Text key0 = Text.wrapCached(key);
        return getPrimaryRecord(PrimaryKey.wrap(record), key0).verify(key0,
                Value.wrap(value));
    }

    @Override
    public boolean verify(String key, TObject value, long record,
            long timestamp) {
        Text key0 = Text.wrapCached(key);
        return getPrimaryRecord(PrimaryKey.wrap(record), key0).verify(key0,
                Value.wrap(value), timestamp);
    }

    /**
     * Return the PrimaryRecord identifier by {@code primaryKey}.
     * 
     * @param pkey
     * @return the PrimaryRecord
     */
    private PrimaryRecord getPrimaryRecord(PrimaryKey pkey) {
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(pkey);
            PrimaryRecord record = cpc.getIfPresent(composite);
            if(record == null) {
                record = Record.createPrimaryRecord(pkey);
                for (Segment segment : segments) {
                    segment.primary().seek(pkey, record);
                }
                cpc.put(composite, record);
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the potentially partial PrimaryRecord identified by {@code key} in
     * {@code primaryKey}.
     * <p>
     * While the returned {@link PrimaryRecord} may not be
     * {@link PrimaryRecord#isPartial() partial}, the caller should interact
     * with it as if it is (e.g. do not perform reads for any other keys besides
     * {@code key}.
     * </p>
     * 
     * @param pkey
     * @param key
     * @return the PrimaryRecord
     */
    private PrimaryRecord getPrimaryRecord(PrimaryKey pkey, Text key) {
        masterLock.readLock().lock();
        try {
            final Composite composite = Composite.create(pkey, key);
            PrimaryRecord record = cppc.getIfPresent(composite);
            if(record == null) {
                // Before loading a partial record, see if the full record is
                // present in memory.
                record = cpc.getIfPresent(Composite.create(pkey));
            }
            if(record == null) {
                record = Record.createPrimaryRecordPartial(pkey, key);
                for (Segment segment : segments) {
                    segment.primary().seek(pkey, key, record);
                }
                cppc.put(composite, record);
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the SearchRecord identified by {@code key}.
     * 
     * @param key
     * @param query
     * @return the SearchRecord
     */
    private SearchRecord getSearchRecord(Text key, Text query) {
        // NOTE: We do not cache SearchRecords because they have the potential
        // to be VERY large. Holding references to them in a cache would prevent
        // them from being garbage collected resulting in more OOMs.
        masterLock.readLock().lock();
        try {
            SearchRecord record = Record.createSearchRecordPartial(key, query);
            for (Segment segment : segments) {
                // Seek each word in the query to make sure that multi word
                // search works.
                String[] toks = query.toString().toLowerCase().split(
                        TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
                for (String tok : toks) {
                    segment.search().seek(key, Text.wrap(tok), record);
                }
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Return the SecondaryRecord identified by {@code key}.
     * 
     * @param key
     * @return the SecondaryRecord
     */
    private SecondaryRecord getSecondaryRecord(Text key) {
        masterLock.readLock().lock();
        try {
            Composite composite = Composite.create(key);
            SecondaryRecord record = csc.getIfPresent(composite);
            if(record == null) {
                record = Record.createSecondaryRecord(key);
                for (Segment segment : segments) {
                    segment.secondary().seek(key, record);
                }
                csc.put(composite, record);
            }
            return record;
        }
        finally {
            masterLock.readLock().unlock();
        }
    }

    /**
     * Create new mutable blocks and sync the current blocks to disk if
     * {@code doSync} is {@code true}.
     * 
     * @param writeToDisk - a flag that controls whether we actually perform a
     *            sync or not. Sometimes this method is called when there is no
     *            data to sync and we just want to create new blocks (e.g. on
     *            initial startup).
     */
    private void triggerSync(boolean writeToDisk) {
        masterLock.writeLock().lock();
        try {
            if(writeToDisk) {
                if(running) {
                    try {
                        seg0.sync(writer);
                    }
                    catch (InterruptedException e) {
                        Logger.warn(
                                "The database was interrupted while trying to "
                                        + "sync storage blocks for {}. Since the blocks "
                                        + "were not fully synced, the contained writes are "
                                        + "still in the Buffer. Any partially synced blocks "
                                        + "will be safely removed when the Database restarts.",
                                seg0.id());
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                else {
                    // The #triggerSync method may be called when the database
                    // is stopped during test cases
                    Logger.warn(
                            "The database is being asked to sync blocks, even though it is not running.");
                    seg0.sync();
                }
            }
            String id = Long.toString(Time.now());
            segments.add((seg0 = Segment.create(id, Paths.get(backingStore))));

        }
        finally {
            masterLock.writeLock().unlock();
        }
    }

    /**
     * A list-view of all the {@link Block Blocks} of type {@code T} within the
     * {@link #segments}. This view is for backwards-compatibility.
     *
     * @author Jeff Nelson
     */
    private class BlockView<T extends Block<?, ?, ?>> extends AbstractList<T> {

        /**
         * A function that converts a segment to the appropriate Block type.
         */
        Function<Segment, T> converter;

        /**
         * Construct a new instance.
         * 
         * @param converter
         */
        BlockView(Function<Segment, T> converter) {
            this.converter = converter;
        }

        @Override
        public T get(int index) {
            Iterator<Segment> it = segments.iterator();
            int count = 0;
            T block = null;
            while (block == null) {
                if(it.hasNext()) {
                    Segment segment = it.next();
                    T _block = converter.apply(segment);
                    if(_block != null) {
                        if(count == index) {
                            block = _block;
                        }
                    }
                    ++count;
                }
                else {
                    break;
                }
            }
            if(block != null) {
                return block;
            }
            else {
                throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public int size() {
            return (int) segments.stream().map(seg -> converter.apply(seg))
                    .filter(block -> block != null).count();
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
            return cpc.getIfPresent(composite) != null;
        }

        @Override
        public boolean contains(String key) {
            Composite composite = Composite.create(Text.wrapCached(key));
            return csc.getIfPresent(composite) != null;
        }

        @Override
        public boolean contains(String key, long record) {
            Composite composite = Composite.create(PrimaryKey.wrap(record),
                    Text.wrapCached(key));
            return cppc.getIfPresent(composite) != null || contains(record);
        }

    }

}
