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
package com.cinchapi.concourse.server.storage;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.annotate.Authorized;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.server.concurrent.LockBroker.Permit;
import com.cinchapi.concourse.server.concurrent.PriorityReadWriteLock;
import com.cinchapi.concourse.server.concurrent.RangeToken;
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
import com.cinchapi.concourse.util.Transformers;
import com.cinchapi.ensemble.Broadcast;
import com.cinchapi.ensemble.EnsembleInstanceIdentifier;
import com.cinchapi.ensemble.Locator;
import com.cinchapi.ensemble.Read;
import com.cinchapi.ensemble.ReturnsEnsemble;
import com.cinchapi.ensemble.WeakRead;
import com.google.common.annotations.VisibleForTesting;

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
public class Engine extends BufferedStore implements
        TransactionSupport,
        AtomicSupport,
        Distributed {

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
     * The {@link LockBroker} that is used to coordinate concurrent operations.
     */
    protected final LockBroker broker; // exposed for Transaction

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
     * Those that have {@link #subscribe(TokenEventObserver) subscribed} to
     * receive {@link #announce(TokenEvent, Token) announcements} about
     * {@link TokenEvent token events}.
     */
    private final Collection<TokenEventObserver> observers = ConcurrentHashMap
            .newKeySet();

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
     * Construct an Engine that is made up of a {@link Buffer} and
     * {@link Database} in the default locations.
     * 
     */
    Engine() {
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
        this.broker = LockBroker.create();
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
    public EnsembleInstanceIdentifier $ensembleInstanceIdentifier() {
        return EnsembleInstanceIdentifier.of(environment);
    }

    @Override
    public LockBroker $ensembleLockBroker() {
        return broker;
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
        boolean accepted = write.getType() == Action.ADD
                ? addUnlocked(write, Sync.of(sync))
                : removeUnlocked(write, Sync.of(sync));
        if(accepted) {
            announce(sharedToken, writeToken, rangeToken);
            Logger.debug("'{}' was accepted by the Engine", write);
        }
        else {
            Logger.warn(
                    "Write {} was rejected by the Engine "
                            + "because it was previously accepted "
                            + "but not offset. This implies that a "
                            + "premature shutdown occurred and the parent"
                            + "Transaction is attempting to restore "
                            + "itself from backup and finish committing.",
                    write);
        }
    }

    @Override
    @com.cinchapi.ensemble.Write
    public boolean add(@Locator String key, TObject value,
            @Locator long record) {
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        Permit shared = broker.writeLock(sharedToken);
        Permit write = broker.writeLock(writeToken);
        Permit range = broker.writeLock(rangeToken);
        try {
            if(addUnlocked(Write.add(key, value, record), Sync.YES)) {
                announce(sharedToken, writeToken, rangeToken);
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            shared.release();
            write.release();
            range.release();
        }
    }

    @Override
    @Restricted
    public void announce(TokenEvent event, Token... tokens) {
        Iterator<TokenEventObserver> it = observers.iterator();
        while (it.hasNext()) {
            TokenEventObserver observer = it.next();
            for (Token token : tokens) {
                if(observer.observe(event, token)) {
                    if(event == TokenEvent.VERSION_CHANGE) {
                        it.remove();
                        break;
                    }
                }
            }
        }
    }

    @Override
    @Read
    public Map<TObject, Set<Long>> browse(@Locator String key) {
        transportLock.readLock().lock();
        RangeToken token = RangeToken.forReading(Text.wrapCached(key),
                Operator.BETWEEN, Value.NEGATIVE_INFINITY,
                Value.POSITIVE_INFINITY);
        Permit range = broker.readLock(token);
        try {
            return super.browse(key);
        }
        finally {
            range.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Map<TObject, Set<Long>> browse(@Locator String key, long timestamp) {
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
    @WeakRead
    public Map<Long, Set<TObject>> chronologize(@Locator String key,
            @Locator long record, long start, long end) {
        transportLock.readLock().lock();
        Token token = Token.wrap(record);
        Permit read = broker.readLock(token);
        try {
            return super.chronologize(key, record, start, end);
        }
        finally {
            read.release();
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
    @Read
    public boolean contains(@Locator long record) {
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
    @Read
    public Map<Long, Set<TObject>> explore(@Locator String key,
            Aliases aliases) {
        transportLock.readLock().lock();
        RangeToken token = RangeToken.forReading(Text.wrapCached(key),
                aliases.operator(), Transformers.transformArray(
                        aliases.values(), Value::wrap, Value.class));
        Permit range = broker.readLock(token);
        try {
            return super.explore(key, aliases);
        }
        finally {
            range.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Map<Long, Set<TObject>> explore(@Locator String key, Aliases aliases,
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
    @Read
    public Set<TObject> gather(@Locator String key, @Locator long record) {
        transportLock.readLock().lock();
        Token token = Token.wrap(key, record);
        Permit read = broker.readLock(token);
        try {
            return super.gather(key, record);
        }
        finally {
            read.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Set<TObject> gather(@Locator String key, @Locator long record,
            long timestamp) {
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
    @Read
    @Broadcast
    // @Reduce(null) // TODO: need to define a reducer
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
    @com.cinchapi.ensemble.Write
    public boolean remove(@Locator String key, TObject value,
            @Locator long record) {
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        Permit shared = broker.writeLock(sharedToken);
        Permit write = broker.writeLock(writeToken);
        Permit range = broker.writeLock(rangeToken);
        try {
            if(removeUnlocked(Write.remove(key, value, record), Sync.YES)) {
                announce(sharedToken, writeToken, rangeToken);
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            shared.release();
            write.release();
            range.release();
        }
    }

    @Override
    @ManagedOperation
    public void repair() {
        transportLock.writeLock().lock();
        try {
            Logger.info("Attempting to repair the '{}' environment",
                    environment);
            super.repair();
        }
        finally {
            transportLock.writeLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Map<Long, List<String>> review(@Locator long record) {
        transportLock.readLock().lock();
        Token token = Token.shareable(record);
        Permit read = broker.readLock(token);
        try {
            return super.review(record);
        }
        finally {
            read.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Map<Long, List<String>> review(@Locator String key,
            @Locator long record) {
        transportLock.readLock().lock();
        Token token = Token.wrap(key, record);
        Permit read = broker.readLock(token);
        try {
            return super.review(key, record);
        }
        finally {
            read.release();
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
    @Read
    public Set<Long> search(@Locator String key, String query) {
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
    @Read
    public Map<String, Set<TObject>> select(@Locator long record) {
        transportLock.readLock().lock();
        Token token = Token.shareable(record);
        Permit read = broker.readLock(token);
        try {
            return super.select(record);
        }
        finally {
            read.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Map<String, Set<TObject>> select(@Locator long record,
            long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.select(record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    @Read
    public Set<TObject> select(@Locator String key, @Locator long record) {
        transportLock.readLock().lock();
        Token token = Token.wrap(key, record);
        Permit read = broker.readLock(token);
        try {
            return super.select(key, record);
        }
        finally {
            read.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public Set<TObject> select(@Locator String key, @Locator long record,
            long timestamp) {
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
    @com.cinchapi.ensemble.Write
    public void set(@Locator String key, TObject value, @Locator long record) {
        Token sharedToken = Token.shareable(record);
        Token writeToken = Token.wrap(key, record);
        RangeToken rangeToken = RangeToken.forWriting(Text.wrap(key),
                Value.wrap(value));
        Permit shared = broker.writeLock(sharedToken);
        Permit write = broker.writeLock(writeToken);
        Permit range = broker.writeLock(rangeToken);
        try {
            super.set(key, value, record);
            announce(sharedToken, writeToken, rangeToken);
        }
        finally {
            shared.release();
            write.release();
            range.release();
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
                            && TimeUnit.MILLISECONDS.convert(Time.now()
                                    - bufferTransportThreadLastWakeUp.get(),
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
    @com.cinchapi.ensemble.Write
    @Broadcast
    @ReturnsEnsemble
    public AtomicOperation startAtomicOperation(String id) {
        return AtomicOperation.start(this, broker, id);
    }

    @Override
    @com.cinchapi.ensemble.Write
    @Broadcast
    @ReturnsEnsemble
    public Transaction startTransaction(String id) {
        return Transaction.start(this, id);
    }

    @Override
    public void stop() {
        if(running) {
            running = false;
            scheduler.cancel();
            limbo.stop();
            bufferTransportThread.interrupt();
            durable.stop();
            broker.shutdown();
            observers.clear();
        }
    }

    @Override
    @Restricted
    public void subscribe(TokenEventObserver observer) {
        observers.add(observer);
    }

    @Override
    public void sync() {
        limbo.sync();
    }

    @Override
    @Restricted
    public void unsubscribe(TokenEventObserver observer) {
        observers.remove(observer);
    }

    @Override
    @Read
    public boolean verify(@Locator Write write) {
        transportLock.readLock().lock();
        Token token = Token.wrap(write.getKey().toString(),
                write.getRecord().longValue());
        Permit read = broker.readLock(token);
        try {
            return super.verify(write);
        }
        finally {
            read.release();
            transportLock.readLock().unlock();
        }
    }

    @Override
    @WeakRead
    public boolean verify(@Locator Write write, long timestamp) {
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
     * Returns {@code true} if this {@link Engine}
     * {@link #announce(TokenEvent, Token...) announces} {@link TokenEvent token
     * events} to {@code observer}.
     * 
     * @param observer
     * @return {@code true} if {@code observer} is
     *         {@link #subscribe(TokenEventObserver) subscribed} to this
     *         {@link Engine}
     */
    @VisibleForTesting
    boolean containsTokenEventObserver(TokenEventObserver observer) {
        return observers.contains(observer);
    }

    /**
     * Add the {@code write} WITHOUT grabbing any locks.
     * 
     * @param write
     * @param sync
     * @return {@code true} if the add was successful
     */
    private boolean addUnlocked(Write write, Sync sync) {
        // NOTE: #sync ends up being NO when the Engine accepts
        // Writes that are transported from a committing AtomicOperation
        // or Transaction, in which case passing this boolean along to
        // the Buffer allows group sync to happen. Similarly, #verify should
        // also be NO during group sync because the Writes have already been
        // verified prior to commit.
        Verify verify = sync == Sync.YES ? Verify.YES : Verify.NO;
        return super.add(write, sync, verify);
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
     * Remove the {@code write} WITHOUT grabbing any locks.
     * 
     * @param write
     * @param sync
     * @return {@code true} if the remove was successful
     */
    private boolean removeUnlocked(Write write, Sync sync) {
        // NOTE: #sync ends up being NO when the Engine accepts
        // Writes that are transported from a committing AtomicOperation
        // or Transaction, in which case passing this boolean along to
        // the Buffer allows group sync to happen. Similarly, #verify should
        // also be NO during group sync because the Writes have already been
        // verified prior to commit.
        Verify verify = sync == Sync.YES ? Verify.YES : Verify.NO;
        return super.remove(write, sync, verify);
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
