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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.server.storage.temp.ToggleQueue;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.TObject.Aliases;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.ensemble.Ensemble;
import com.cinchapi.ensemble.EnsembleInstanceIdentifier;
import com.cinchapi.ensemble.core.LocalProcess;

/**
 * An {@link AtomicOperation} that performs backups prior to commit to make sure
 * that it is durable in the event of crash, power loss or failure.
 * 
 * @implNote Internally uses a {@link ToggleQueue} to ensure that a logical
 *           {@link Write} topic isn't needlessly toggled (e.g. ADD X, REMOVE
 *           X, ADD X, etc)
 * 
 * @author Jeff Nelson
 */
public final class Transaction extends AtomicOperation implements
        AtomicSupport,
        Ensemble {
    // NOTE: Because Transaction's rely on JIT locking, the unsafe methods call
    // the safe counterparts in the super class (AtomicOperation) because those
    // have logic to tell the BufferedStore class to perform unsafe reads.

    /**
     * Return the Transaction for {@code destination} that is backed up to
     * {@code file}. This method will finish committing the transaction before
     * returning.
     * 
     * @param destination
     * @param file
     * @return The restored {@link Transaction}
     */
    public static void recover(Engine destination, String file) {
        try {
            ByteBuffer bytes = FileSystem.map(file, MapMode.READ_ONLY, 0,
                    FileSystem.getFileSize(file));
            Transaction transaction = new Transaction(destination, bytes);
            transaction.invokeSuperDoCommit(true); // recovering transaction
                                                   // must always syncAndVerify
                                                   // to prevent possible data
                                                   // duplication
            FileSystem.deleteFile(file);
        }
        catch (Exception e) {
            Logger.warn("Attempted to recover a transaction from {}, "
                    + "but the data is corrupted. This indicates that "
                    + "Concourse Server shutdown before the transaction "
                    + "could properly commit, so none of the data "
                    + "in the transaction has persisted.", file);
            Logger.debug(
                    "Transaction backup in {} is corrupt because " + "of {}",
                    file, e);
            FileSystem.deleteFile(file);
        }
    }

    /**
     * Return a new Transaction with {@code engine} as the eventual destination.
     * 
     * @param engine
     * @return the new Transaction
     */
    public static Transaction start(Engine engine) {
        return new Transaction(engine);
    }

    /**
     * The unique Transaction id.
     */
    private final String id;

    /**
     * Whenever an {@link AtomicOperation} is {@link #startAtomicOperation()
     * started}, it, by virtue of being a {@link TokenEventObserver},
     * {@link #subscribe(TokenEventObserver) subscribes} for announcements about
     * token events from this {@link Transaction}. The {@link Transaction}
     * {@link #observe(TokenEvent, Token) observes} on behalf of the
     * {@link AtomicOperation}, intercepting announcements from its own
     * destination store (e.g., the {@link Engine}) and
     * {@link AtomicOperation#abort() aborting} the {@link AtomicOperation} if
     * it {@link AtomicOperation#interrupts(Token, TokenEvent) concerns} an
     * {@link #observe(TokenEvent, Token) observed} {@link TokenEvent event} for
     * a {@link Token}
     * <p>
     * This collection is non thread-safe because it is assumed that only one
     * {@link AtomicOperation} will live at a time, so there will ever only be
     * one {@link #observers observer}.
     * </p>
     */
    private final Set<TokenEventObserver> observers = new HashSet<>(1);

    /**
     * A handler that bypasses the parent {@link AtomicOperation} logic and
     * hooks into the "unlocked" logic of {@link BufferedStore} for read
     * methods. This handler facilitates the methods that are defined in
     * {@link LockFreeStore}.
     */
    private final BufferedStore unlocked;

    /**
     * Construct a new instance.
     */
    Transaction() {
        super(null, LockBroker.noOp());
        this.id = null;
        this.unlocked = null;
    }

    /**
     * Construct a new instance.
     * 
     * @param destination
     */
    private Transaction(Engine destination) {
        super(new ToggleQueue(INITIAL_CAPACITY), destination,
                destination.broker);
        this.id = Long.toString(Time.now());
        this.unlocked = new BufferedStore(limbo, durable) {

            @Override
            public void start() {}

            @Override
            public void stop() {}

            @Override
            protected boolean verifyWithReentrancy(Write write) {
                return super.verify(write);
            }

        };
    }

    /**
     * Construct a new instance.
     * 
     * @param destination
     * @param bytes
     */
    private Transaction(Engine destination, ByteBuffer bytes) {
        this(destination);
        deserialize(bytes);
        setStatus(Status.COMMITTED);

    }

    @Override
    public void $ensembleAbortAtomic(EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = LocalProcess.instance().get(identifier);
        atomic.abort();
    }

    @Override
    public void $ensembleFinishCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = LocalProcess.instance().get(identifier);
        atomic.finish();
    }

    @Override
    public EnsembleInstanceIdentifier $ensembleInstanceIdentifier() {
        return EnsembleInstanceIdentifier.of(id);
    }

    @Override
    public boolean $ensemblePrepareCommitAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = LocalProcess.instance().get(identifier);
        return atomic.commit();
    }

    @Override
    public EnsembleInstanceIdentifier $ensembleStartAtomic(
            EnsembleInstanceIdentifier identifier) {
        TwoPhaseCommit atomic = new TwoPhaseCommit(identifier, this,
                ((Engine) durable).broker);
        return atomic.$ensembleInstanceIdentifier();
    }

    @Override
    public void abort() {
        super.abort();
        Logger.info("Aborted Transaction {}", this);
    }

    @Override
    public void accept(Write write) {
        // Accept writes from an AtomicOperation and put them in this
        // Transaction's buffer without performing an additional #verify, but
        // grabbing the necessary lock intentions.
        checkArgument(write.getType() != Action.COMPARE);
        if(write.getType() == Action.ADD) {
            add(write, Sync.NO, Verify.NO);
        }
        else {
            remove(write, Sync.NO, Verify.NO);
        }
    }

    @Override
    public void accept(Write write, boolean sync) {
        accept(write);

    }

    @Override
    public void announce(TokenEvent event, Token... tokens) {}

    @Override
    public Map<TObject, Set<Long>> browseUnlocked(String key) {
        return unlocked.browse(key);
    }

    @Override
    public Map<Long, Set<TObject>> chronologizeUnlocked(String key, long record,
            long start, long end) {
        return unlocked.chronologize(key, record, start, end);
    }

    @Override
    public Map<Long, Set<TObject>> exploreUnlocked(String key,
            Aliases aliases) {
        return unlocked.explore(key, aliases);
    }

    @Override
    public Set<TObject> gatherUnlocked(String key, long record) {
        return unlocked.gather(key, record);
    }

    @Override
    public boolean observe(TokenEvent event, Token token) {
        // We override this method to handle the case where an Atomic Operation
        // started from this Transaction must fail because of a version change,
        // but that failure should not cause the transaction itself to fail
        // (i.e. calling verifyAndSwap from a transaction and a version change
        // causes that particular operation to fail prior to commit. The logic
        // in this method will simply cause the invocation of verifyAndSwap to
        // return false while this transaction would stay alive.
        while (observers == null) {
            // Account for a race condition where the Transaction (via
            // AtomicOperation) subscribes for TokenEvents before the #observers
            // collection is set during Transaction construction
            Logger.warn("A Transaction handled by {} received a Token "
                    + "Event announcement before it was fully initialized",
                    Thread.currentThread());
            Thread.yield();
        }
        boolean intercepted = false;
        try {
            for (TokenEventObserver observer : observers) {
                AtomicOperation atomic = (AtomicOperation) observer;
                if(atomic.isPreemptedBy(event, token)) {
                    atomic.abort();
                    intercepted = true;
                }
            }
        }
        catch (ConcurrentModificationException e) {
            // Another asynchronous write or announcement was received while
            // observing the token event, so a retry is necessary.
            return observe(event, token);
        }
        if(intercepted) {
            return true;
        }
        else {
            return super.observe(event, token);
        }
    }

    @Override
    public void onCommit(AtomicOperation operation) {
        absorb(operation);
    }

    @Override
    public Map<Long, List<String>> reviewUnlocked(long record) {
        return unlocked.review(record);
    }

    @Override
    public Map<Long, List<String>> reviewUnlocked(String key, long record) {
        return unlocked.review(key, record);
    }

    @Override
    public Map<String, Set<TObject>> selectUnlocked(long record) {
        return unlocked.select(record);
    }

    @Override
    public Set<TObject> selectUnlocked(String key, long record) {
        return unlocked.select(key, record);
    }

    @Override
    public AtomicOperation startAtomicOperation() {
        checkState();
        // A Transaction is, itself, an AtomicOperation that must adhere to the
        // JIT Locking guarantee with respect to the Engine's lock services, so
        // if it births an AtomicOperation, it should just inherit but defer any
        // locks needed therewithin, instead of passing the Engine's lock
        // broker on
        return AtomicOperation.start(this, LockBroker.noOp());
    }

    @Override
    public void subscribe(TokenEventObserver observer) {
        observers.add(observer);
    }

    @Override
    public void sync() {/* no-op */}

    @Override
    public String toString() {
        return id;
    }

    @Override
    public void unsubscribe(TokenEventObserver observer) {
        observers.remove(observer);
    }

    @Override
    public boolean verifyUnlocked(String key, TObject value, long record) {
        return unlocked.verify(key, value, record);
    }

    @Override
    public boolean verifyUnlocked(Write write) {
        return unlocked.verify(write);
    }

    @Override
    protected void checkIfQueuedPreempted() throws AtomicStateException {
        try {
            super.checkIfQueuedPreempted();
        }
        catch (AtomicStateException e) {
            throw new TransactionStateException();
        }
    }

    @Override
    protected void checkState() throws AtomicStateException {
        try {
            super.checkState();
        }
        catch (AtomicStateException e) {
            throw new TransactionStateException();
        }
    }

    @Override
    protected void doCommit() {
        if(isReadOnly()) {
            invokeSuperDoCommit(false);
        }
        else {
            String file = ((Engine) durable).transactionStore + File.separator
                    + id + ".txn";
            FileChannel channel = FileSystem.getFileChannel(file);
            try {
                channel.write(serialize());
                channel.force(true);
                Logger.info("Created backup for transaction {} at '{}'", this,
                        file);
                invokeSuperDoCommit(false);
                FileSystem.deleteFile(file);
            }
            catch (IOException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
            finally {
                FileSystem.closeFileChannel(channel);
            }
        }
    }

    @Override
    @Restricted
    protected boolean isPreemptedBy(TokenEvent event, Token token) {
        for (TokenEventObserver observer : observers) {
            if(((AtomicOperation) observer).isPreemptedBy(event, token)) {
                return true;
            }
        }
        return super.isPreemptedBy(event, token);
    }

    /**
     * Deserialize the content of this Transaction from {@code bytes}.
     * 
     * @param bytes
     */
    private void deserialize(ByteBuffer bytes) {
        locks = new HashSet<>();
        Iterator<ByteBuffer> it = ByteableCollections
                .iterator(ByteBuffers.slice(bytes, bytes.getInt()));
        while (it.hasNext()) {
            LockDescription lock = LockDescription.fromByteBuffer(it.next(),
                    broker);
            // TODO: grab the lock?
            locks.add(lock);
        }
        it = ByteableCollections.iterator(bytes);
        while (it.hasNext()) {
            Write write = Write.fromByteBuffer(it.next());
            limbo.insert(write);
        }
    }

    /**
     * Invoke {@link #doCommit()} that is defined in the super class. This
     * method should only be called when it is desirable to doCommit without
     * performing a backup (i.e. when restoring from a backup in a static
     * method).
     * 
     * @param syncAndVerify a flag that is passed onto the
     *            {@link AtomicOperation#doCommit(boolean)} method
     */
    private void invokeSuperDoCommit(boolean syncAndVerify) {
        super.doCommit(syncAndVerify);
        Logger.info("Finalized commit for Transaction {}", this);
    }

    /**
     * Serialize the Transaction to a ByteBuffer.
     * <ol>
     * <li><strong>lockSize</strong> - position 0</li>
     * <li><strong>locks</strong> - position 4</li>
     * <li><strong>writes</strong> - position 4 + lockSize</li>
     * </ol>
     * 
     * @return the ByteBuffer representation
     */
    private ByteBuffer serialize() {
        ByteBuffer _locks = ByteableCollections.toByteBuffer(locks);
        ByteBuffer _writes = ByteableCollections
                .toByteBuffer(((Queue) limbo).getWrites());
        ByteBuffer bytes = ByteBuffer
                .allocate(4 + _locks.capacity() + _writes.capacity());
        bytes.putInt(_locks.capacity());
        bytes.put(_locks);
        bytes.put(_writes);
        bytes.rewind();
        return bytes;
    }
}
