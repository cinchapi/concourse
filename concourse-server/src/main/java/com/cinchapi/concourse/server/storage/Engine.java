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

import static com.cinchapi.concourse.server.GlobalState.ENABLE_BATCH_TRANSPORTS;
import static com.cinchapi.concourse.server.GlobalState.USE_FAIR_TRANSPORT_LOCK;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.annotate.Authorized;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.collect.Iterators;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.server.concurrent.LockBroker.Permit;
import com.cinchapi.concourse.server.concurrent.Locks;
import com.cinchapi.concourse.server.concurrent.RangeToken;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ManagedOperation;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.server.storage.transporter.BatchTransporter;
import com.cinchapi.concourse.server.storage.transporter.StreamingTransporter;
import com.cinchapi.concourse.server.storage.transporter.Transporter;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.concourse.util.Transformers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
     * Create the appropriate value for {@link #transportLock} depending upon
     * the preferred {@link Transporter}.
     * 
     * @return the value for {@link #transportLock}
     */
    @SuppressWarnings("deprecation")
    private static ReentrantReadWriteLock createTransportLock() {
        if(ENABLE_BATCH_TRANSPORTS) {
            return new ReentrantReadWriteLock(USE_FAIR_TRANSPORT_LOCK);
        }
        else {
            return com.cinchapi.concourse.server.concurrent.PriorityReadWriteLock
                    .prioritizeReads();
        }
    }

    /**
     * The id used to determine that the Buffer should be dumped in the
     * {@link #dump(String)} method.
     */
    public static final String BUFFER_DUMP_ID = "BUFFER";

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
     * The {@link Transporter} responsible for transporting {@link Write Writes}
     * from the {@link BufferedStore#limbo buffer} to the
     * {@link BufferedStore#durable database}.
     */
    protected Transporter transporter; // visible for Testing

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
     * A lock that prevents the Engine from causing the Buffer to transport
     * Writes to the Database while a buffered read is occurring. Even though
     * the Buffer has a transportLock, we must also maintain one at the Engine
     * level to prevent the appearance of dropped writes where data is
     * transported from the Buffer to the Database after the Database context is
     * captured and sent to the Buffer to finish the buffered reading.
     */
    private final ReentrantReadWriteLock transportLock = createTransportLock();

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
    public boolean add(String key, TObject value, long record) {
        transportLock.readLock().lock();
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
            transportLock.readLock().unlock();
        }
    }

    @Override
    public ReadWriteLock advisoryLock() {
        // Higher level abstractions (e.g., the 'Stores' factory class and
        // Atomic Operations in Concourse Server) call multiple Engine
        // primitives. Each primitive acquires the transport read lock to
        // prevent state changes during execution, but releases it immediately
        // after completion. This creates contention as transports can
        // interleave between primitive calls within a single logical operation.
        //
        // By exposing the transportLock as an advisoryLock, higher level
        // operations can acquire it once at the beginning and hold it
        // throughout their execution, ensuring the entire bulk/atomic operation
        // proceeds without interference from background transports.
        return transportLock;
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
    public Map<TObject, Set<Long>> browse(String key) {
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
    public boolean remove(String key, TObject value, long record) {
        transportLock.readLock().lock();
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
            transportLock.readLock().unlock();
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
            List<Iterator<Write>> iterators = ImmutableList
                    .of(((Database) durable).iterator(), limbo.iterator());
            for (Iterator<Write> it : iterators) {
                /*
                 * For each store, catalog every Write in the inventory, in case
                 * there are inconsistencies.
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
    public Map<Long, List<String>> review(String key, long record) {
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
        transportLock.readLock().lock();
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
            transportLock.readLock().unlock();
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
            transporter = buildTransporter();
            Logger.info("The Engine is using the {} transporter to index data",
                    ENABLE_BATCH_TRANSPORTS ? "Batch" : "Streaming");
            transporter.start();
        }
    }

    @Override
    public AtomicOperation startAtomicOperation() {
        return AtomicOperation.start(this, broker);
    }

    @Override
    public Transaction startTransaction() {
        return Transaction.start(this);
    }

    @Override
    public void stop() {
        if(running) {
            running = false;
            transporter.stop();
            limbo.stop();
            durable.stop();
            broker.shutdown();
            observers.clear();
            transporter = null;
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
    public boolean verify(Write write) {
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
        Locks.lockIfCondition(transportLock.readLock(), verify == Verify.YES);
        try {
            return super.add(write, sync, verify);
        }
        finally {
            Locks.unlockIfCondition(transportLock.readLock(),
                    verify == Verify.YES);
        }
    }

    /**
     * Construct the appropriate {@link Transporter} based on system
     * configuration.
     * 
     * @return the configured {@link Transporter} instance
     */
    private Transporter buildTransporter() {
        Buffer buffer = (Buffer) limbo;
        Database database = (Database) durable;
        Lock lock = transportLock.writeLock();
        Transporter transporter;
        if(ENABLE_BATCH_TRANSPORTS) {
            AwaitableExecutorService segmentWriter = Reflection.get("writer",
                    database); /* (authorized) */
            Preconditions.checkState(segmentWriter != null);
            // @formatter:off
            transporter = BatchTransporter.from(buffer).to(database)
                    .withLock(lock)
                    .withSegmentWriter(segmentWriter)
                    .environment(environment)
                    .build();
            // @formatter:on
        }
        else {
            transporter = new StreamingTransporter(buffer, database,
                    environment, lock);
        }
        return transporter;
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
        Locks.lockIfCondition(transportLock.readLock(), verify == Verify.YES);
        try {
            return super.remove(write, sync, verify);
        }
        finally {
            Locks.unlockIfCondition(transportLock.readLock(),
                    verify == Verify.YES);
        }
    }
}
