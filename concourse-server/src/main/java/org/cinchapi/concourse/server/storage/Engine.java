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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.Authorized;
import org.cinchapi.concourse.annotate.DoNotInvoke;
import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.concurrent.LockService;
import org.cinchapi.concourse.server.concurrent.RangeLockService;
import org.cinchapi.concourse.server.concurrent.Threads;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.jmx.ManagedOperation;
import org.cinchapi.concourse.server.storage.db.Database;
import org.cinchapi.concourse.server.storage.temp.Buffer;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Logger;

import com.beust.jcommander.internal.Sets;

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
     * The thread that is responsible for transporting buffer content in the
     * background.
     */
    private final Thread bufferTransportThread; // NOTE: Having a dedicated
                                                // thread that sleeps is faster
                                                // than using an
                                                // ExecutorService.

    /**
     * The location where transaction backups are stored.
     */
    protected final String transactionStore; // exposed for Transaction backup

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
    private final ReentrantReadWriteLock transportLock = new ReentrantReadWriteLock();

    /**
     * A collection of listeners that should be notified of a version change for
     * a given token.
     */
    @SuppressWarnings("serial")
    private final Map<Token, Set<VersionChangeListener>> versionChangeListeners = new ConcurrentHashMap<Token, Set<VersionChangeListener>>() {

        /**
         * An empty set that is returned on calls to {@link #get(Object)} when
         * there key does not exist.
         */
        private final Set<VersionChangeListener> emptySet = Collections
                .unmodifiableSet(Sets.<VersionChangeListener> newHashSet());

        @Override
        public Set<VersionChangeListener> get(Object key) {
            if(containsKey(key)) {
                return super.get(key);
            }
            else {
                return emptySet;
            }

        }

    };

    /**
     * Construct an Engine that is made up of a {@link Buffer} and
     * {@link Database} in the default locations.
     * 
     */
    public Engine() {
        this(new Buffer(), new Database());
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
        this(new Buffer(bufferStore), new Database(dbStore));
    }

    /**
     * Construct an Engine that is made up of {@code buffer} and
     * {@code database}.
     * 
     * @param buffer
     * @param database
     */
    @Authorized
    private Engine(Buffer buffer, Database database) {
        super(buffer, database);
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
        boolean accepted = write.getType() == Action.ADD ? add(key, value,
                record) : remove(key, value, record);
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
        LockService.getWriteLock(key, record).lock();
        RangeLockService.getWriteLock(key, value).lock();
        try {
            if(super.add(key, value, record)) {
                notifyVersionChange(Token.wrap(key, record));
                notifyVersionChange(Token.wrap(record));
                return true;
            }
            return false;
        }
        finally {
            LockService.getWriteLock(key, record).unlock();
            RangeLockService.getWriteLock(key, value).unlock();
        }
    }

    @Override
    @Restricted
    public void addVersionChangeListener(Token token,
            VersionChangeListener listener) {
        Set<VersionChangeListener> listeners = null;
        if(!versionChangeListeners.containsKey(token)) {
            listeners = Sets.newHashSet();
            versionChangeListeners.put(token, listeners);
        }
        else {
            listeners = versionChangeListeners.get(token);
        }
        listeners.add(listener);
    }

    @Override
    public Map<Long, String> audit(long record) {
        transportLock.readLock().lock();
        LockService.getReadLock(record).lock();
        try {
            return super.audit(record);
        }
        finally {
            LockService.getReadLock(record).unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Map<Long, String> audit(String key, long record) {
        transportLock.readLock().lock();
        LockService.getReadLock(key, record).lock();
        try {
            return super.audit(key, record);
        }
        finally {
            LockService.getReadLock(key, record).unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<String> describe(long record) {
        transportLock.readLock().lock();
        LockService.getReadLock(record).lock();
        try {
            return super.describe(record);
        }
        finally {
            LockService.getReadLock(record).unlock();
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<String> describe(long record, long timestamp) {
        transportLock.readLock().lock();
        try {
            return super.describe(record, timestamp);
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
        LockService.getReadLock(key, record).lock();
        try {
            return super.fetch(key, record);
        }
        finally {
            LockService.getReadLock(key, record).unlock();
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
    public Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values) {
        transportLock.readLock().lock();
        try {
            return super.find(timestamp, key, operator, values);
        }
        finally {
            transportLock.readLock().unlock();
        }
    }

    @Override
    public Set<Long> find(String key, Operator operator, TObject... values) {
        transportLock.readLock().lock();
        RangeLockService.getReadLock(key, operator, values).lock();
        try {
            return super.find(key, operator, values);
        }
        finally {
            RangeLockService.getReadLock(key, operator, values).unlock();
            transportLock.readLock().unlock();
        }
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
        for (VersionChangeListener listener : versionChangeListeners.get(token)) {
            listener.onVersionChange(token);
        }
    }

    @Override
    public boolean remove(String key, TObject value, long record) {
        LockService.getWriteLock(key, record).lock();
        RangeLockService.getWriteLock(key, value).lock();
        try {
            if(super.remove(key, value, record)) {
                notifyVersionChange(Token.wrap(key, record));
                notifyVersionChange(Token.wrap(record));
                return true;
            }
            return false;
        }
        finally {
            LockService.getWriteLock(key, record).unlock();
            RangeLockService.getWriteLock(key, value).unlock();
        }
    }

    @Override
    @Restricted
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener) {
        versionChangeListeners.get(token).remove(listener);
    }

    @Override
    public Set<Long> search(String key, String query) {
        // NOTE: Range locking for a search query requires too much overhead, so
        // we must be willing to live with the fact that a search query may
        // provide inconsistent results if a match is added while the read is
        // processing.
        return super.search(key, query);
    }

    @Override
    public void start() {
        if(!running) {
            Logger.info("Starting the Engine...");
            running = true;
            buffer.start();
            destination.start();
            doTransactionRecovery();
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
            buffer.stop();
            destination.stop();
        }
    }

    @Override
    public boolean verify(String key, TObject value, long record) {
        transportLock.readLock().lock();
        LockService.getReadLock(key, record).lock();
        try {
            return super.verify(key, value, record);
        }
        finally {
            LockService.getReadLock(key, record).unlock();
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
            super("BufferTransport");
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running) {
                if(transportLock.writeLock().tryLock()) {
                    buffer.transport(destination);
                    transportLock.writeLock().unlock();
                }
                Threads.sleep(5);
            }
        }
    }
}
