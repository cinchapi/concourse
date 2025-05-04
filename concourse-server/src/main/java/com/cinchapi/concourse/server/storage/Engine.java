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
package com.cinchapi.concourse.server.storage;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.annotate.Authorized;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.collect.Iterators;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.LockService;
import com.cinchapi.concourse.server.concurrent.PriorityReadWriteLock;
import com.cinchapi.concourse.server.concurrent.RangeLockService;
import com.cinchapi.concourse.server.concurrent.RangeToken;
import com.cinchapi.concourse.server.concurrent.RangeTokens;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * The {@code Engine} schedules concurrent CRUD operations, manages ACID
 * transactions, versions writes and indexes data.
 * <p>
 * The Engine is a {@link BufferedStore}. Writing to the {@link Database} is
 * expensive because multiple index records must be deserialized, updated and
 * flushed back to disk for each revision. By using a {@link Buffer}, the Engine
 * can handle writes in a more efficient manner with minimal impact on Read
 * performance. The buffering system provides full CD guarantees.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public final class Engine extends BufferedStore implements
        TransactionSupport,
        AtomicSupport {

    //
    // NOTES ON LOCKING:
    // =================
    // Even though the individual storage components (Segment, Record, etc)
    // handle their own locking, we must also grab "global" coordinating locks
    // in the Engine
    //
    // 1) to account for the fact that an atomic operation may lock notions of
    // things to create a virtual fence that ensures the atomicity of its
    // reads and writes, and
    //
    // 2) BufferedStore operations query the buffer and destination separately
    // and the individual locking protocols of those stores are not sufficient
    // to prevent dropped data (i.e. while reading from the destination it is
    // okay to continue writing to the buffer since we'll lock there when its
    // time BUT, between the time that we unlock in the #destination and the
    // time when we lock in the #buffer, it is possible for a transport from the
    // #buffer to the #destination to occur in which case the data would be
    // dropped since it wasn't read during the #destination query and won't be
    // read during the #buffer scan).
    //
    // It is important to note that we DO NOT need to do any locking for
    // historical reads because concurrent data writes cannot affect what is
    // returned.

    /**
     * The id used to determine that the Buffer should be dumped in the
     * {@link #dump(String)} method.
     */
    public static final String BUFFER_DUMP_ID = "BUFFER";

    /**
     * The number of milliseconds we allow between writes before pausing the
     * {@link BufferTransportThread}. If the amount of time between writes is
     * less than this value then we assume we are streaming writes, which means
     * it is more efficient for the BufferTransportThread to busy-wait than
     * block.
     */
    protected static final int BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS = 1000; // visible
                                                                                                              // for
                                                                                                              // testing

    /**
     * The frequency with which we check to see if the
     * {@link BufferTransportThread} has hung/stalled.
     */
    protected static int BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS = 10000; // visible
                                                                                                   // for
                                                                                                   // testing

    /**
     * The number of milliseconds we allow the {@link BufferTransportThread} to
     * sleep without waking up (e.g. being in the TIMED_WAITING) state before we
     * assume that the thread has hung/stalled and we try to rescue it.
     */
    protected static int BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS = 5000; // visible
                                                                                                 // for
                                                                                                 // testing

    /**
     * A flag to indicate that the {@link BufferTransportThrread} has appeared
     * to be hung at some point during the current runtime.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverAppearedHung = new AtomicBoolean(
            false); // visible for testing

    /**
     * A flag to indicate that the {@link BufferTransportThread} has ever been
     * successfully restarted after appearing to be hung during the current
     * runtime.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverBeenRestarted = new AtomicBoolean(
            false); // visible for testing

    /**
     * A flag to indicate that the {@link BufferTransportThread} has, at least
     * once, gone into "paused" mode where it blocks during inactivity instead
     * of busy waiting.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverPaused = new AtomicBoolean(
            false); // visible for testing

    /**
     * If this value is > 0, then we will sleep for this amount instead of what
     * the buffer suggests. This is mainly used for testing.
     */
    protected int bufferTransportThreadSleepInMs = 0; // visible for testing

    /**
     * The inventory contains a collection of all the records that have ever
     * been created. The Engine and its Buffer share access to this inventory so
     * that the Buffer can update it whenever a new record is written. The
     * Engine uses the inventory to make some reads (i.e. verify) more
     * efficient.
     */
    protected final Inventory inventory; // visible for testing

    /**
     * The {@link LockService} that is used to coordinate concurrent operations.
     */
    protected final LockService lockService; // exposed for Transaction

    /**
     * The {@link RangeLockService} that is used to coordinate concurrent
     * operations.
     */
    protected final RangeLockService rangeLockService; // exposed for Transction

    /**
     * The location where transaction backups are stored.
     */
    protected final String transactionStore; // exposed for Transaction backup

    /**
     * The thread that is responsible for transporting buffer content in the
     * background.
     */
    private Thread bufferTransportThread; // NOTE: Having a dedicated
                                          // thread that sleeps is faster
                                          // than using an
                                          // ExecutorService.

    /**
     * A flag that indicates whether the {@link BufferTransportThread} is
     * actively doing work at the moment. This flag is necessary so we don't
     * interrupt the thread if it appears to be hung when it is actually just
     * busy doing a lot of work.
     */
    private final AtomicBoolean bufferTransportThreadIsDoingWork = new AtomicBoolean(
            false);

    /**
     * A flag that indicates that the {@link BufferTransportThread} is currently
     * paused due to inactivity (e.g. no writes).
     */
    private final AtomicBoolean bufferTransportThreadIsPaused = new AtomicBoolean(
            false);

    /**
     * The timestamp when the {@link BufferTransportThread} last awoke from
     * sleep. We use this to help monitor and detect whether the thread has
     * stalled/hung.
     */
    private final AtomicLong bufferTransportThreadLastWakeUp = new AtomicLong(
            Time.now());

    /**
     * The environment that is associated with this {@link Engine}.
     */
    private final String environment;

    /**
     * A collection of listeners that should be notified of a version change for
     * a given range token.
     */
    private final Cache<VersionChangeListener, Map<Text, RangeSet<Value>>> rangeVersionChangeListeners = CacheBuilder
            .newBuilder().weakKeys().build();

    /**
     * A flag to indicate if the Engine is running or not.
     */
    private volatile boolean running = false;

    /**
     * A {@link Timer} that is used to schedule some regular tasks.
     */
    private Timer scheduler;

    /**
     * A lock that prevents the Engine from causing the Buffer to transport
     * Writes to the Database while a buffered read is occurring. Even though
     * the Buffer has a transportLock, we must also maintain one at the Engine
     * level to prevent the appearance of dropped writes where data is
     * transported from the Buffer to the Database after the Database context is
     * captured and sent to the Buffer to finish the buffered reading.
     */
    private final ReentrantReadWriteLock transportLock = PriorityReadWriteLock
            .prioritizeReads();

    /**
     * A collection of listeners that should be notified of a version change for
     * a given token.
     */
    private final ConcurrentMap<Token, WeakHashMap<VersionChangeListener, Boolean>> versionChangeListeners = new ConcurrentHashMap<Token, WeakHashMap<VersionChangeListener, Boolean>>();

    /**
     * Construct an Engine that is made up of a {@link Buffer} and
     * {@link Database} in the default locations.
     * 
     */
    public Engine() {
        this(new Buffer(), new Database(), GlobalState.DEFAULT_ENVIRONMENT);
    }

    /**
     * Construct an Engine that is made up of a {@link Buffer} and
     * {@link Database} that are both backed by {@code bufferStore} and
     * {@code dbStore} respectively.
     * 
     * @param bufferStore
     * @param dbStore
     */
    public Engine(String bufferStore, String dbStore) {
        this(bufferStore, dbStore, GlobalState.DEFAULT_ENVIRONMENT);
    }

    /**
     * Construct an Engine that is made up of a {@link Buffer} and
     * {@link Database} that are both backed by {@code bufferStore} and
     * {@code dbStore} respectively} and are associated with {@code environment}
     * .
     * 
     * @param bufferStore
     * @param dbStore
     * @param environment
     */
    public Engine(String bufferStore, String dbStore, String environment) {
        this(new Buffer(bufferStore), new Database(dbStore), environment);
    }

    /**
     * Construct an Engine that is made up of {@code buffer} and
     * {@code database}.
     * 
     * @param buffer
     * @param database
     * @param environment
     */
    @Authorized
    private Engine(Buffer buffer, Database database, String environment) {
        super(buffer, database);
        this.lockService = LockService.create();
        this.rangeLockService = RangeLockService.create();
        this.environment = environment;
        this.transactionStore = buffer.getBackingStore() + File.separator
                + "txn"; /* (authorized) */
        this.inventory = Inventory.create(buffer.getBackingStore()
                + File.separator + "meta" + File.separator + "inventory");
        buffer.setInventory(inventory);
        buffer.setThreadNamePrefix(environment + "-buffer");
        buffer.setEnvironment(environment);
        database.tag(environment);
    }

    @Override
    @DoNotInvoke
    public void accept(Write write) {
        accept(write, true);
    }

    /**
     * <p>
     * The Engine is the destination for Transaction commits, which means that
     * this method will accept Writes from Transactions and create new Writes
     * within the Engine BufferedStore (e.g. a new Write will be created in the
     * Buffer and eventually transported to the Database). Creating a new Write
     * does associate a new timestamp with the transactional data, but this is
     * the desired behaviour because data from a Transaction should always have
     * a post commit timestamp.
     * </p>
     * <p>
     * It is also worth calling out the fact that this method does not have any
     * locks to prevent multiple transactions from concurrently invoking meaning
     * that two transactions that don't have any overlapping reads/writes
     * (meaning they don't touch any of the same data) can commit at the same
     * time and its possible that their writes will be interleaved since calls
     * to this method don't globally lock. This is <em>okay</em> and does not
     * violate ACID because the <strong>observed</strong> state of the system
     * will be the same as if the transactions transported all their Writes
     * serially.
     * </p>
     */
    @Override
    @DoNotInvoke
    public void accept(Write write, boolean sync) {
        checkArgument(write.getType() != Action.COMPARE);
        String key = write.getKey().toString();
        TObject value = write.getValue().getTObject();
        long record = write.getRecord().longValue();
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        boolean accepted;
        if(write.getType() == Action.ADD) {
            accepted = addUnlocked(write, sync ? Sync.YES : Sync.NO,
                    sharedToken, writeToken, rangeToken);
        }
        else {
            accepted = removeUnlocked(write, sync ? Sync.YES : Sync.NO,
                    sharedToken, writeToken, rangeToken);
        }
        if(!accepted) {
            Logger.warn(
                    "Write {} was rejected by the Engine "
                            + "because it was previously accepted "
                            + "but not offset. This implies that a "
                            + "premature shutdown occurred and the parent"
                            + "Transaction is attempting to restore "
                            + "itself from backup and finish committing.",
                    write);
        }
        else {
            Logger.debug("'{}' was accepted by the Engine", write);
        }
    }

    @Override
    public boolean add(String key, TObject value, long record) {
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        Lock shared = lockService.getWriteLock(sharedToken);
        Lock write = lockService.getWriteLock(writeToken);
        Lock range = rangeLockService.getWriteLock(rangeToken);
        shared.lock();
        write.lock();
        range.lock();
        try {
            return addUnlocked(Write.add(key, value, record), Sync.YES,
                    sharedToken, writeToken, rangeToken);
        }
        finally {
            shared.unlock();
            write.unlock();
            range.unlock();
        }
    }

    @Override
    public ReadWriteLock advisoryLock() {
        // Higher level abstractions (in Stores factory class) and atomic
        // operations in Concourse Server call multiple Engine primitives. Each
        // primitive acquires the transport read lock to prevent state changes
        // during execution, but releases it immediately after completion. This
        // creates contention as transports can interleave between primitive
        // calls within a single logical operation.
        //
        // By exposing the transportLock as an advisoryLock, higher level
        // operations can acquire it once at the beginning and hold it
        // throughout their execution, ensuring the entire bulk/atomic operation
        // proceeds without interference from background transports.
        return transportLock;
    }

    @Override
    @Restricted
    public void addVersionChangeListener(Token token,
            VersionChangeListener listener) {
        if(token instanceof RangeToken) {
            Iterable<Range<Value>> ranges = RangeTokens
                    .convertToRange((RangeToken) token);
            for (Range<Value> range : ranges) {
                Map<Text, RangeSet<Value>> map = rangeVersionChangeListeners
                        .getIfPresent(listener);
                if(map == null) {
                    map = Maps.newHashMap();
                    rangeVersionChangeListeners.put(listener, map);
                }
                RangeSet<Value> set = map.get(((RangeToken) token).getKey());
                if(set == null) {
                    set = TreeRangeSet.create();
                    map.put(((RangeToken) token).getKey(), set);
                }
                set.add(range);
            }
        }
        else {
            WeakHashMap<VersionChangeListener, Boolean> existing = versionChangeListeners
                    .get(token);
            if(existing == null) {
                WeakHashMap<VersionChangeListener, Boolean> created = new WeakHashMap<VersionChangeListener, Boolean>();
                existing = versionChangeListeners.putIfAbsent(token, created);
                existing = MoreObjects.firstNonNull(existing, created);
            }
            synchronized (existing) {
                existing.put(listener, Boolean.TRUE);
            }
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        transportLock.readLock().lock();
        Lock range = rangeLockService.getReadLock(Text.wrapCached(key),
                Operator.BETWEEN, Value.NEGATIVE_INFINITY,
                Value.POSITIVE_INFINITY);
        range.lock();
        try {
            return super.browse(key);
        }
        finally {
            range.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.browse(key, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browseUnlocked(String key) {
        transportLock.readLock().lock();
        try {
            return super.browse(key);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, Set<TObject>> chronologize(String key, long record,
            long start, long end) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(record);
        read.lock();
        try {
            return super.chronologize(key, record, start, end);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, Set<TObject>> chronologizeUnlocked(String key, long record,
            long start, long end) {
        transportLock.readLock().lock();
        try {
            return super.chronologize(key, record, start, end);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    @ManagedOperation
    public void compact() {
        durable.compact();
        limbo.compact();
    }

    @Override
    public boolean contains(long record) {
        return inventory.contains(record);
    }

    /**
     * Public interface for the {@link Database#dump(String)} method.
     * 
     * @param id
     * @return the block dumps
     */
    @ManagedOperation
    public String dump(String id) {
        if(id.equalsIgnoreCase(BUFFER_DUMP_ID)) {
            return ((Buffer) limbo).dump();
        }
        return ((Database) durable).dump(id);
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases) {
        transportLock.readLock().lock();
        Lock range = rangeLockService.getReadLock(key, aliases.operator(),
                aliases.values());
        range.lock();
        try {
            return super.explore(key, aliases);
        }
        finally {
            range.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, Set<TObject>> explore(String key, Aliases aliases,
            long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.explore(key, aliases, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, Set<TObject>> exploreUnlocked(String key,
            Aliases aliases) {
        transportLock.readLock().lock();
        try {
            return super.explore(key, aliases);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> gather(String key, long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key, record);
        read.lock();
        try {
            return super.gather(key, record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> gather(String key, long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.gather(key, record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> gatherUnlocked(String key, long record) {
        transportLock.readLock().lock();
        try {
            return super.gather(key, record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<Long> getAllRecords() {
        return inventory.getAll();
    }

    /**
     * Public interface for the {@link Database#getDumpList()} method.
     * 
     * @return the dump list
     */
    @ManagedOperation
    public String getDumpList() {
        List<String> ids = ((Database) durable).getDumpList();
        ids.add("BUFFER");
        ListIterator<String> it = ids.listIterator(ids.size());
        StringBuilder sb = new StringBuilder();
        while (it.hasPrevious()) {
            sb.append(Math.abs(it.previousIndex() - ids.size()));
            sb.append(") ");
            sb.append(it.previous());
            sb.append(System.getProperty("line.separator"));
        }
        return sb.toString();
    }

    @Override
    @Restricted
    public void notifyVersionChange(Token token) {
        if(token instanceof RangeToken) {
            Iterable<Range<Value>> ranges = RangeTokens
                    .convertToRange((RangeToken) token);
            for (Entry<VersionChangeListener, Map<Text, RangeSet<Value>>> entry : rangeVersionChangeListeners
                    .asMap().entrySet()) {
                VersionChangeListener listener = entry.getKey();
                RangeSet<Value> set = entry.getValue()
                        .get(((RangeToken) token).getKey());
                for (Range<Value> range : ranges) {
                    if(set != null && !set.subRangeSet(range).isEmpty()) {
                        listener.onVersionChange(token);
                    }
                }
            }
        }
        else {
            WeakHashMap<VersionChangeListener, Boolean> existing = versionChangeListeners
                    .get(token);
            if(existing != null) {
                synchronized (existing) {
                    Iterator<VersionChangeListener> it = existing.keySet()
                            .iterator();
                    while (it.hasNext()) {
                        VersionChangeListener listener = it.next();
                        listener.onVersionChange(token);
                        it.remove();
                    }
                }
            }
        }
    }

    @Override
    public boolean remove(String key, TObject value, long record) {
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        Lock shared = lockService.getWriteLock(sharedToken);
        Lock write = lockService.getWriteLock(writeToken);
        Lock range = rangeLockService.getWriteLock(rangeToken);
        shared.lock();
        write.lock();
        range.lock();
        try {
            return removeUnlocked(Write.remove(key, value, record), Sync.YES,
                    sharedToken, writeToken, rangeToken);
        }
        finally {
            shared.unlock();
            write.unlock();
            range.unlock();
        }
    }

    @Override
    @Restricted
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener) {
        // NOTE: Since we use weak references listeners, we don't have to do
        // manual cleanup because the GC will take care of it.
    }

    @Override
    @ManagedOperation
    public void repair() {
        transportLock.writeLock().lock();
        try {
            Logger.info("Attempting to repair the '{}' environment",
                    environment);
            super.repair();
            List<Iterator<Write>> iterators = ImmutableList
                    .of(((Database) durable).iterator(), limbo.iterator());
            for (Iterator<Write> it : iterators) {
                /*
                 * For each store, (re)catalog every Write in the inventory, in
                 * case there are inconsistencies.
                 * 
                 * Note: We intentionally reuse the existing inventory instead
                 * of creating a new one. This approach is safe because:
                 * - Any extra records in the inventory (that don't exist in
                 * actual data) will only trigger extraneous verifies
                 * - These extra lookups don't affect the consistency of the
                 * observed state
                 */
                try {
                    while (it.hasNext()) {
                        long record = it.next().getRecord().longValue();
                        inventory.add(record);
                    }
                }
                finally {
                    Iterators.close(it);
                }
            }
            inventory.sync();
        }
        finally {
            transportLock.writeLock().unlock();
        }
    }

    @Override
    public Map<Long, List<String>> review(long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(Token.shareable(record));
        read.lock();
        try {
            return super.review(record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, List<String>> review(String key, long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key, record);
        read.lock();
        try {
            return super.review(key, record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, List<String>> reviewUnlocked(long record) {
        transportLock.readLock().lock();
        try {
            return super.review(record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, List<String>> reviewUnlocked(String key, long record) {
        transportLock.readLock().lock();
        try {
            return super.review(key, record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<Long> search(String key, String query) {
        // NOTE: Range locking for a search query requires too much overhead, so
        // we must be willing to live with the fact that a search query may
        // provide inconsistent results if a match is added while the read is
        // processing.
        transportLock.readLock().lock();
        try {
            return super.search(key, query);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> select(long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(Token.shareable(record));
        read.lock();
        try {
            return super.select(record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.select(record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> select(String key, long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key, record);
        read.lock();
        try {
            return super.select(key, record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.select(key, record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> selectUnlocked(long record) {
        transportLock.readLock().lock();
        try {
            return super.select(record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> selectUnlocked(String key, long record) {
        transportLock.readLock().lock();
        try {
            return super.select(key, record);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public void set(String key, TObject value, long record) {
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        Lock shared = lockService.getWriteLock(sharedToken);
        Lock write = lockService.getWriteLock(writeToken);
        Lock range = rangeLockService.getWriteLock(rangeToken);
        shared.lock();
        write.lock();
        range.lock();
        try {
            super.set(key, value, record);
            notifyVersionChange(writeToken);
            notifyVersionChange(sharedToken);
            notifyVersionChange(rangeToken);
        }
        finally {
            shared.unlock();
            write.unlock();
            range.unlock();
        }
    }

    @Override
    public void start() {
        if(!running) {
            Logger.info("Starting the '{}' Engine...", environment);
            running = true;
            durable.start();
            limbo.start();
            durable.reconcile(limbo.hashes());
            doTransactionRecovery();
            bufferTransportThread = new BufferTransportThread();
            scheduler = new Timer(true);
            scheduler.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    if(!bufferTransportThreadIsDoingWork.get()
                            && !bufferTransportThreadIsPaused.get()
                            && bufferTransportThreadLastWakeUp.get() != 0
                            && TimeUnit.MILLISECONDS.convert(
                                    Time.now() - bufferTransportThreadLastWakeUp
                                            .get(),
                                    TimeUnit.MICROSECONDS) > BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS) {
                        bufferTransportThreadHasEverAppearedHung.set(true);
                        bufferTransportThread.interrupt();
                    }

                }

            }, BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS,
                    BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS);
            bufferTransportThread.start();
        }
    }

    @Override
    public AtomicOperation startAtomicOperation() {
        return AtomicOperation.start(this, lockService, rangeLockService);
    }

    @Override
    public Transaction startTransaction() {
        return Transaction.start(this);
    }

    @Override
    public void stop() {
        if(running) {
            running = false;
            scheduler.cancel();
            limbo.stop();
            bufferTransportThread.interrupt();
            durable.stop();
            lockService.shutdown();
            rangeLockService.shutdown();
        }
    }

    @Override
    public void sync() {
        limbo.sync();
    }

    @Override
    public boolean verify(Write write) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(write.getKey().toString(),
                write.getRecord().longValue());
        read.lock();
        try {
            return super.verify(write);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.verify(write, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public boolean verifyUnlocked(Write write) {
        transportLock.readLock().lock();
        try {
            return super.verify(write);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    protected boolean verifyWithReentrancy(Write write) {
        return super.verify(write);
    }

    /**
     * Add {@code key} as {@code value} to {@code record} WITHOUT grabbing any
     * locks. This method is ONLY appropriate to call from the
     * {@link #accept(Write)} method that processes transaction commits since,
     * in that case, the appropriate locks have already been grabbed.
     * 
     * @param write
     * @param sync
     * @param sharedToken - {@link LockToken} for record
     * @param writeToken - {@link LockToken} for key in record
     * @param rangeToken - {@link RangeToken} for writing value to key
     * @return {@code true} if the add was successful
     */
    private boolean addUnlocked(Write write, Sync sync, Token sharedToken,
            Token writeToken, RangeToken rangeToken) {
        // NOTE: #sync ends up being NO when the Engine accepts
        // Writes that are transported from a committing AtomicOperation
        // or Transaction, in which case passing this boolean along to
        // the Buffer allows group sync to happen. Similarly, #verify should
        // also be NO during group sync because the Writes have already been
        // verified prior to commit.
        Verify verify = sync == Sync.YES ? Verify.YES : Verify.NO;
        if(super.add(write, sync, verify)) {
            notifyVersionChange(writeToken);
            notifyVersionChange(sharedToken);
            notifyVersionChange(rangeToken);
            return true;
        }
        return false;
    }

    /**
     * Restore any transactions that did not finish committing prior to the
     * previous shutdown.
     */
    private void doTransactionRecovery() {
        FileSystem.mkdirs(transactionStore);
        for (File file : new File(transactionStore).listFiles()) {
            Transaction.recover(this, file.getAbsolutePath());
            Logger.info("Restored Transaction from {}", file.getName());
        }
    }

    /**
     * Return the number of milliseconds that have elapsed since the last time
     * the {@link BufferTransportThread} successfully transported data.
     * 
     * @return the idle time
     */
    private long getBufferTransportThreadIdleTimeInMs() {
        return TimeUnit.MILLISECONDS.convert(
                Time.now() - ((Buffer) limbo).getTimeOfLastTransport(),
                TimeUnit.MICROSECONDS);
    }

    /**
     * Remove {@code key} as {@code value} from {@code record} WITHOUT grabbing
     * any locks. This method is ONLY appropriate to call from the
     * {@link #accept(Write)} method that processes transaction commits since,
     * in that case, the appropriate locks have already been grabbed.
     * 
     * @param key
     * @param value
     * @param record
     * @param sync
     * @param sharedToken - {@link LockToken} for record
     * @param writeToken - {@link LockToken} for key in record
     * @param rangeToken - {@link RangeToken} for writing value to key
     * @return {@code true} if the remove was successful
     */
    private boolean removeUnlocked(Write write, Sync sync, Token sharedToken,
            Token writeToken, RangeToken rangeToken) {
        // NOTE: #sync ends up being NO when the Engine accepts
        // Writes that are transported from a committing AtomicOperation
        // or Transaction, in which case passing this boolean along to
        // the Buffer allows group sync to happen. Similarly, #verify should
        // also be NO during group sync because the Writes have already been
        // verified prior to commit.
        Verify verify = sync == Sync.YES ? Verify.YES : Verify.NO;
        if(super.remove(write, sync, verify)) {
            notifyVersionChange(writeToken);
            notifyVersionChange(sharedToken);
            notifyVersionChange(rangeToken);
            return true;
        }
        return false;
    }

    /**
     * A thread that is responsible for transporting content from
     * {@link #limbo} to {@link #durable}.
     * 
     * @author Jeff Nelson
     */
    private class BufferTransportThread extends Thread {

        /**
         * Construct a new instance.
         */
        public BufferTransportThread() {
            super(AnyStrings.joinSimple("BufferTransport [", environment, "]"));
            setDaemon(true);
            setPriority(MIN_PRIORITY);
            setUncaughtExceptionHandler((thread, exception) -> {
                Logger.error("Uncaught exception in {}:", thread.getName(),
                        exception);
                Logger.error(
                        "{} has STOPPED WORKING due to an unexpected exception. Writes will accumulate in the buffer without being transported until the error is resolved",
                        thread.getName());
            });
        }

        @Override
        public void run() {
            while (running) {
                if(Thread.interrupted()) { // the thread has been
                                           // interrupted from the Engine
                                           // stopping
                    break;
                }
                if(getBufferTransportThreadIdleTimeInMs() > BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS) {
                    // If there have been no transports within the last second
                    // then make this thread block until the buffer is
                    // transportable so that we do not waste CPU cycles
                    // busy waiting unnecessarily.
                    bufferTransportThreadHasEverPaused.set(true);
                    bufferTransportThreadIsPaused.set(true);
                    Logger.debug(
                            "Paused the background data transport thread because "
                                    + "it has been inactive for at least {} milliseconds",
                            BUFFER_TRANSPORT_THREAD_ALLOWABLE_INACTIVITY_THRESHOLD_IN_MILLISECONDS);
                    limbo.waitUntilTransportable();
                    if(Thread.interrupted()) { // the thread has been
                                               // interrupted from the Engine
                                               // stopping
                        break;
                    }
                }
                doTransport();
                try {
                    // NOTE: This thread needs to sleep for a small amount of
                    // time to avoid thrashing
                    int sleep = bufferTransportThreadSleepInMs > 0
                            ? bufferTransportThreadSleepInMs
                            : limbo.getDesiredTransportSleepTimeInMs();
                    Thread.sleep(sleep);
                    bufferTransportThreadLastWakeUp.set(Time.now());
                }
                catch (InterruptedException e) {
                    if(getBufferTransportThreadIdleTimeInMs() > BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS) {
                        Logger.warn(
                                "The data transport thread been sleeping for over "
                                        + "{} milliseconds even though there is work to do. "
                                        + "An attempt has been made to restart the stalled "
                                        + "process.",
                                BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS);
                        bufferTransportThreadHasEverBeenRestarted.set(true);
                    }
                    else {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        /**
         * Tell the Buffer to transport data and prevent deadlock in the event
         * of failure.
         */
        private void doTransport() {
            if(transportLock.writeLock().tryLock()) {
                try {
                    bufferTransportThreadIsPaused.compareAndSet(true, false);
                    bufferTransportThreadIsDoingWork.set(true);
                    limbo.transport(durable);
                    bufferTransportThreadLastWakeUp.set(Time.now());
                    bufferTransportThreadIsDoingWork.set(false);
                }
                finally {
                    transportLock.writeLock().unlock();
                }
            }

        }
    }
}
