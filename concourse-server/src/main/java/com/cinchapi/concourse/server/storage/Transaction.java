/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.annotate.Restricted;
import com.cinchapi.concourse.server.concurrent.LockService;
import com.cinchapi.concourse.server.concurrent.RangeLockService;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * An {@link AtomicOperation} that performs backups prior to commit to make sure
 * that it is durable in the event of crash, power loss or failure.
 * 
 * @author Jeff Nelson
 */
public final class Transaction extends AtomicOperation implements AtomicSupport {
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
     * @return The restored Transaction
     */
    public static void recover(Engine destination, String file) {
        try {
            Transaction transaction = new Transaction(destination,
                    FileSystem.map(file, MapMode.READ_ONLY, 0,
                            FileSystem.getFileSize(file)));
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
            Logger.debug("Transaction backup in {} is corrupt because "
                    + "of {}", file, e);
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
     * The Transaction "manages" the version change listeners for each of its
     * Atomic Operations. Since the Transaction is registered with the Engine
     * for version change notifications for each action of each of its atomic
     * operations, the Transaction must intercept any notifications that would
     * affect an atomic operation that has not committed.
     */
    private Multimap<AtomicOperation, Token> managedVersionChangeListeners = HashMultimap
            .create();

    /**
     * The unique Transaction id.
     */
    private final String id;

    /**
     * Construct a new instance.
     * 
     * @param destination
     */
    private Transaction(Engine destination) {
        super(new Queue(INITIAL_CAPACITY), destination);
        this.id = Long.toString(Time.now());
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
        open.set(false);
    }

    @Override
    public void abort() {
        super.abort();
        Logger.info("Aborted Transaction {}", this);
    }

    @Override
    public void accept(Write write) {
        // Accept writes from an AtomicOperation and put them in this
        // Transaction's buffer.
        checkArgument(write.getType() != Action.COMPARE);
        String key = write.getKey().toString();
        TObject value = write.getValue().getTObject();
        long record = write.getRecord().longValue();
        if(write.getType() == Action.ADD) {
            add(key, value, record);
        }
        else {
            remove(key, value, record);
        }
    }

    @Override
    public void accept(Write write, boolean sync) {
        accept(write);

    }

    @Override
    @Restricted
    public void addVersionChangeListener(Token token,
            VersionChangeListener listener) {
        // The Transaction is added as a version change listener for each of its
        // atomic operation reads/writes by virtue of the fact that the atomic
        // operations (via BufferedStore) call the analogous read/write methods
        // in the Transaction, which registers the Transaction with
        // the Engine as a version change listener.
        managedVersionChangeListeners.put((AtomicOperation) listener, token);
    }

    @Override
    public Map<Long, String> auditUnsafe(long record) {
        return audit(record);
    }

    @Override
    public Map<Long, String> auditUnsafe(String key, long record) {
        return audit(key, record);
    }
    
    @Override
    public Map<Long, Set<TObject>> chronologizeUnsafe(String key, long record,
            long start, long end) {
        return chronologize(key, record, start, end);
    }

    @Override
    public Map<String, Set<TObject>> browseUnsafe(long record) {
        return select(record);
    }

    @Override
    public Map<TObject, Set<Long>> browseUnsafe(String key) {
        return browse(key);
    }

    @Override
    public Map<Long, Set<TObject>> doExploreUnsafe(String key,
            Operator operator, TObject... values) {
        return doExplore(key, operator, values);
    }

    @Override
    public Set<TObject> selectUnsafe(String key, long record) {
        return select(key, record);
    }

    @Override
    @Restricted
    public void notifyVersionChange(Token token) {}

    @Override
    public void onVersionChange(Token token) {
        // We override this method to handle the case where an atomic operation
        // started from this transaction must fail because of a version change,
        // but that failure should not cause the transaction itself to fail
        // (i.e. calling verifyAndSwap from a transaction and a version change
        // causes that particular operation to fail prior to commit. The logic
        // in this method will simply cause the invocation of verifyAndSwap to
        // return false while this transaction would stay alive.
        boolean callSuper = true;
        for (AtomicOperation operation : managedVersionChangeListeners.keySet()) {
            for (Token tok : managedVersionChangeListeners.get(operation)) {
                if(tok.equals(token)) {
                    operation.onVersionChange(tok);
                    managedVersionChangeListeners.remove(operation, tok);
                    callSuper = false;
                    break;
                }
            }
        }
        if(callSuper) {
            super.onVersionChange(token);
        }
    }

    @Override
    @Restricted
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener) {}

    @Override
    public AtomicOperation startAtomicOperation() {
        checkState();
        AtomicOperation operation = AtomicOperation.start(this);
        operation.lockService = LockService.noOp();
        operation.rangeLockService = RangeLockService.noOp();
        return operation;
    }

    @Override
    public void sync() {/* no-op */}

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean verifyUnsafe(String key, TObject value, long record) {
        return verify(key, value, record);
    }

    /**
     * Deserialize the content of this Transaction from {@code bytes}.
     * 
     * @param bytes
     */
    private void deserialize(ByteBuffer bytes) {
        locks = Maps.newHashMap();
        Iterator<ByteBuffer> it = ByteableCollections.iterator(ByteBuffers
                .slice(bytes, bytes.getInt()));
        while (it.hasNext()) {
            LockDescription lock = LockDescription.fromByteBuffer(it.next(),
                    lockService, rangeLockService);
            locks.put(lock.getToken(), lock);
        }
        it = ByteableCollections.iterator(bytes);
        while (it.hasNext()) {
            Write write = Write.fromByteBuffer(it.next());
            buffer.insert(write);
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
        ByteBuffer _locks = ByteableCollections.toByteBuffer(locks.values());
        ByteBuffer _writes = ByteableCollections.toByteBuffer(((Queue) buffer)
                .getWrites());
        ByteBuffer bytes = ByteBuffer.allocate(4 + _locks.capacity()
                + _writes.capacity());
        bytes.putInt(_locks.capacity());
        bytes.put(_locks);
        bytes.put(_writes);
        bytes.rewind();
        return bytes;
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
            String file = ((Engine) destination).transactionStore
                    + File.separator + id + ".txn";
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
                throw Throwables.propagate(e);
            }
            finally {
                FileSystem.closeFileChannel(channel);
            }
        }
    }

    /**
     * Perform cleanup for the atomic {@code operation} that was birthed from
     * this transaction and has successfully committed.
     * 
     * @param operation an AtomicOperation, birthed from this Transaction,
     *            that has committed successfully
     */
    protected void onCommit(AtomicOperation operation) {
        managedVersionChangeListeners.removeAll(operation);
    }

}
