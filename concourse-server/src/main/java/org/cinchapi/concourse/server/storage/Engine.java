/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

import java.io.File;
import java.text.MessageFormat;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.common.util.NonBlockingHashMultimap;
import org.cinchapi.common.util.NonBlockingRangeMap;
import org.cinchapi.common.util.Range;
import org.cinchapi.common.util.RangeMap;
import org.cinchapi.concourse.annotate.Authorized;
import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.GlobalState;
import org.cinchapi.concourse.server.concurrent.LockService;
import org.cinchapi.concourse.server.concurrent.PriorityReadWriteLock;
import org.cinchapi.concourse.server.concurrent.RangeLockService;
import org.cinchapi.concourse.server.concurrent.RangeToken;
import org.cinchapi.concourse.server.concurrent.RangeTokens;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.jmx.ManagedOperation;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.db.Database;
import org.cinchapi.concourse.server.storage.temp.Buffer;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Logger;

import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.*;

/**
 * The {@code Engine} schedules concurrent CRUD operations, manages ACID
 * transactions, versions writes and indexes data.
 * <p>
 * The Engine is a {@link BufferedStore}. Writing to the {@link Database} is
 * expensive because multiple index records must be deserialized, updated and
 * flushed back to disk for each revision. By using a {@link Buffer}, the Engine
 * can handle writes in a more efficient manner which minimal impact on Read
 * performance. The buffering system provides full CD guarantees.
 * </p>
 * 
 * @author jnelson
 */
@ThreadSafe
public final class Engine extends BufferedStore implements
        Transactional,
        Compoundable {

    //
    // NOTES ON LOCKING:
    // =================
    // Even though the individual storage components (Block, Record, etc)
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
     * The id used to determine that the Buffer should be dumped.
     */
    public static final String BUFFER_DUMP_ID = "BUFFER";

    /**
     * The number of milliseconds we allow between writes before we pause the
     * {@link BufferTransportThread}. If there amount of time between writes is
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
     * If this value is > 0, then we will sleep for this amount instead of what
     * the buffer suggests. This is mainly used for testing.
     */
    protected int bufferTransportThreadSleepInMs = 0; // visible for testing

    /**
     * A flag to indicate that the {@link BufferTransportThrread} has appeared
     * to be hung at some point during the current lifecycle.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverAppearedHung = new AtomicBoolean(
            false); // visible for testing

    /**
     * A flag to indicate that the {@link BufferTransportThread} has ever been
     * sucessfully restarted after appearing to be hung during the current
     * lifecycle.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverBeenRestarted = new AtomicBoolean(
            false); // visible for testing

    /**
     * A flag to indicate that the {@link BufferTransportThread} has gone into
     * block mode instead of busy waiting at least once.
     */
    protected final AtomicBoolean bufferTransportThreadHasEverPaused = new AtomicBoolean(
            false); // visible for testing

    /**
     * The location where transaction backups are stored.
     */
    protected final String transactionStore; // exposed for Transaction backup

    /**
     * The thread that is responsible for transporting buffer content in the
     * background.
     */
    private final Thread bufferTransportThread; // NOTE: Having a dedicated
                                                // thread that sleeps is faster
                                                // than using an
                                                // ExecutorService.

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
    private final RangeMap<Value, VersionChangeListener> rangeVersionChangeListeners = NonBlockingRangeMap
            .create();

    /**
     * A flag to indicate if the Engine is running or not.
     */
    private volatile boolean running = false;

    /**
     * A {@link Timer} that is used to schedule some regular tasks.
     */
    private final Timer scheduler = new Timer(true);

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
    private final Multimap<Token, VersionChangeListener> versionChangeListeners = NonBlockingHashMultimap
            .create();

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
        super(buffer, database, LockService.create(), RangeLockService.create());
        this.environment = environment;
        this.bufferTransportThread = new BufferTransportThread();
        this.transactionStore = buffer.getBackingStore() + File.separator
                + "txn"; /* (authorized) */
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
    public void accept(Write write) {
        checkArgument(write.getType() != Action.COMPARE);
        String key = write.getKey().toString();
        TObject value = write.getValue().getTObject();
        long record = write.getRecord().longValue();
        boolean accepted = write.getType() == Action.ADD ? addUnsafe(key,
                value, record, false) : removeUnsafe(key, value, record, false);
        if(!accepted) {
            Logger.warn("Write {} was rejected by the Engine "
                    + "because it was previously accepted "
                    + "but not offset. This implies that a "
                    + "premature shutdown occurred and the parent"
                    + "Transaction is attempting to restore "
                    + "itself from backup and finish committing.", write);
        }
        else {
            Logger.debug("'{}' was accepted by the Engine", write);
        }
    }

    @Override
    public boolean add(String key, TObject value, long record) {
        Lock write = lockService.getWriteLock(key, record);
        Lock range = rangeLockService.getWriteLock(key, value);
        write.lock();
        range.lock();
        try {
            return addUnsafe(key, value, record);
        }
        finally {
            write.unlock();
            range.unlock();
        }
    }

    @Override
    @Restricted
    public void addVersionChangeListener(Token token,
            VersionChangeListener listener) {
        if(token instanceof RangeToken) {
            Iterable<Range<Value>> ranges = RangeTokens
                    .convertToRange((RangeToken) token);
            for (Range<Value> range : ranges) {
                rangeVersionChangeListeners.put(range, listener);
            }
        }
        else {
            versionChangeListeners.put(token, listener);
        }
    }

    @Override
    public Map<Long, String> audit(long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(record);
        read.lock();
        try {
            return super.audit(record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key, record);
        read.lock();
        try {
            return super.audit(key, record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> browse(long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(record);
        read.lock();
        try {
            return super.browse(record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Set<TObject>> browse(long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.browse(record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<TObject, Set<Long>> browse(String key) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key);
        read.lock();
        try {
            return super.browse(key);
        }
        finally {
            read.unlock();
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

    /**
     * Public interface for the {@link Database#dump(String)} method.
     * 
     * @param id
     * @return the block dumps
     */
    @ManagedOperation
    public String dump(String id) {
        if(id.equalsIgnoreCase(BUFFER_DUMP_ID)) {
            return ((Buffer) buffer).dump();
        }
        return ((Database) destination).dump(id);
    }

    @Override
    public Set<TObject> fetch(String key, long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key, record);
        read.lock();
        try {
            return super.fetch(key, record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<TObject> fetch(String key, long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.fetch(key, record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }
    
    @Override
    public void flush() {
        buffer.flush();
    }

    /**
     * Public interface for the {@link Database#getDumpList()} method.
     * 
     * @return the dump list
     */
    @ManagedOperation
    public String getDumpList() {
        List<String> ids = ((Database) destination).getDumpList();
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
    public long getVersion(long record) {
        return Math.max(buffer.getVersion(record),
                ((Database) destination).getVersion(record));
    }

    public long getVersion(String key) {
        return Math.max(buffer.getVersion(key),
                ((Database) destination).getVersion(key));
    }

    @Override
    public long getVersion(String key, long record) {
        return Math.max(buffer.getVersion(key, record),
                ((Database) destination).getVersion(key, record));
    }

    @Override
    @Restricted
    public void notifyVersionChange(Token token) {
        if(token instanceof RangeToken) {
            Iterable<Range<Value>> ranges = RangeTokens
                    .convertToRange((RangeToken) token);
            for (Range<Value> range : ranges) {
                for (VersionChangeListener listener : rangeVersionChangeListeners
                        .get(range)) {
                    listener.onVersionChange(token);
                }
            }
        }
        else {
            for (VersionChangeListener listener : versionChangeListeners
                    .get(token)) {
                listener.onVersionChange(token);
            }
        }
    }

    @Override
    public boolean remove(String key, TObject value, long record) {
        Lock write = lockService.getWriteLock(key, record);
        Lock range = rangeLockService.getWriteLock(key, value);
        write.lock();
        range.lock();
        try {
            return removeUnsafe(key, value, record);
        }
        finally {
            write.unlock();
            range.unlock();
        }
    }

    @Override
    @Restricted
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener) {
        if(token instanceof RangeToken) {
            Iterable<Range<Value>> ranges = RangeTokens
                    .convertToRange((RangeToken) token);
            for (Range<Value> range : ranges) {
                rangeVersionChangeListeners.remove(range, listener);
            }
        }
        else {
            versionChangeListeners.remove(token, listener);
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
    public void set(String key, TObject value, long record) {
        Lock write = lockService.getWriteLock(key, record);
        Lock range = rangeLockService.getWriteLock(key, value);
        write.lock();
        range.lock();
        try {
            super.set(key, value, record);
            notifyVersionChange(Token.wrap(key, record));
            notifyVersionChange(Token.wrap(record));
            notifyVersionChange(Token.wrap(key));
            notifyVersionChange(RangeToken.forWriting(Text.wrap(key),
                    Value.wrap(value)));
        }
        finally {
            write.unlock();
            range.unlock();
        }
    }

    @Override
    public void start() {
        if(!running) {
            Logger.info("Starting the '{}' Engine...", environment);
            running = true;
            destination.start();
            buffer.start();
            doTransactionRecovery();
            scheduler.scheduleAtFixedRate(
                    new TimerTask() {

                        @Override
                        public void run() {
                            if(!bufferTransportThreadIsPaused.get()
                                    && bufferTransportThreadLastWakeUp.get() != 0
                                    && TimeUnit.MILLISECONDS
                                            .convert(
                                                    Time.now()
                                                            - bufferTransportThreadLastWakeUp
                                                                    .get(),
                                                    TimeUnit.MICROSECONDS) > BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_THRESOLD_IN_MILLISECONDS) {
                                bufferTransportThreadHasEverAppearedHung
                                        .set(true);
                                bufferTransportThread.interrupt();
                            }

                        }

                    },
                    BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS,
                    BUFFER_TRANSPORT_THREAD_HUNG_DETECTION_FREQUENCY_IN_MILLISECONDS);
            bufferTransportThread.start();
        }
    }

    @Override
    public AtomicOperation startAtomicOperation() {
        return AtomicOperation.start(this);
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
            buffer.stop();
            bufferTransportThread.interrupt();
            destination.stop();
        }
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        transportLock.readLock().lock();
        Lock read = lockService.getReadLock(key, record);
        read.lock();
        try {
            return super.verify(key, value, record);
        }
        finally {
            read.unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public boolean verify(String key, TObject value, long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.verify(key, value, record, timestamp);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(long timestamp, String key,
            Operator operator, TObject... values) {
        transportLock.readLock().lock();
        try {
            return super.doExplore(timestamp, key, operator, values);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    protected Map<Long, Set<TObject>> doExplore(String key, Operator operator,
            TObject... values) {
        transportLock.readLock().lock();
        Lock range = rangeLockService.getReadLock(key, operator, values);
        range.lock();
        try {
            return super.doExplore(key, operator, values);
        }
        finally {
            range.unlock();
            transportLock.readLock().unlock();
        }
    }

    /**
     * Add {@code key} as {@code value} to {@code record} WITHOUT grabbing any
     * locks. This method is ONLY appropriate to call from the
     * {@link #accept(Write)} method that processes transaction commits since,
     * in that case, the appropriate locks have already been grabbed.
     * 
     * @param key
     * @param value
     * @param record
     * @return {@code true} if the add was successful
     */
    private boolean addUnsafe(String key, TObject value, long record) {
        return addUnsafe(key, value, record, true);
    }

    /**
     * Add {@code key} as {@code value} to {@code record} WITHOUT grabbing any
     * locks. This method is ONLY appropriate to call from the
     * {@link #accept(Write)} method that processes transaction commits since,
     * in that case, the appropriate locks have already been grabbed.
     * 
     * @param key
     * @param value
     * @param record
     * @param flush
     * @return {@code true} if the add was successful
     */
    private boolean addUnsafe(String key, TObject value, long record,
            boolean flush) {
        if(super.add(key, value, record, flush)) {
            notifyVersionChange(Token.wrap(key, record));
            notifyVersionChange(Token.wrap(record));
            notifyVersionChange(Token.wrap(key));
            notifyVersionChange(RangeToken.forWriting(Text.wrapCached(key),
                    Value.wrap(value)));
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
                Time.now() - ((Buffer) buffer).getTimeOfLastTransport(),
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
     * @return {@code true} if the add was successful
     */
    private boolean removeUnsafe(String key, TObject value, long record) {
        return removeUnsafe(key, value, record, true);
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
     * @param flush
     * @return {@code true} if the add was successful
     */
    private boolean removeUnsafe(String key, TObject value, long record,
            boolean flush) {
        if(super.remove(key, value, record, flush)) {
            notifyVersionChange(Token.wrap(key, record));
            notifyVersionChange(Token.wrap(record));
            notifyVersionChange(Token.wrap(key));
            notifyVersionChange(RangeToken.forWriting(Text.wrapCached(key),
                    Value.wrap(value)));
            return true;
        }
        return false;
    }

    /**
     * A thread that is responsible for transporting content from
     * {@link #buffer} to {@link #destination}.
     * 
     * @author jnelson
     */
    private class BufferTransportThread extends Thread {

        /**
         * Construct a new instance.
         */
        public BufferTransportThread() {
            super(MessageFormat.format("BufferTransport [{0}]", environment));
            setDaemon(true);
            setPriority(MIN_PRIORITY);
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
                    buffer.waitUntilTransportable();
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
                    int sleep = bufferTransportThreadSleepInMs > 0 ? bufferTransportThreadSleepInMs
                            : buffer.getDesiredTransportSleepTimeInMs();
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
                    buffer.transport(destination);
                }
                finally {
                    transportLock.writeLock().unlock();
                }
            }

        }
    }
}
