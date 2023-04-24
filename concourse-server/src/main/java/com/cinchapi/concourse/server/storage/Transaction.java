/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashSet;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.concurrent.LockBroker;
import com.cinchapi.concourse.server.io.ByteableCollections;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.temp.Queue;
import com.cinchapi.concourse.server.storage.temp.ToggleQueue;
import com.cinchapi.concourse.server.storage.temp.Write;
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
@NotThreadSafe
public final class Transaction extends AtomicOperation implements Ensemble {

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
            transaction.invokeSuperApply(true); // recovering transaction
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
     * Construct a new instance.
     */
    Transaction() {
        super(null, LockBroker.noOp());
        this.id = null;
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
    public String toString() {
        return id;
    }

    @Override
    protected void apply() {
        if(isReadOnly()) {
            invokeSuperApply(false);
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
                invokeSuperApply(false);
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

    protected void throwAtomicStateException() {
        throw new TransactionStateException();
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
     * Invoke {@link #apply()} that is defined in the super class. This
     * method should only be called when it is desirable to doCommit without
     * performing a backup (i.e. when restoring from a backup in a static
     * method).
     * 
     * @param syncAndVerify a flag that is passed onto the
     *            {@link AtomicOperation#apply(boolean)} method
     */
    private void invokeSuperApply(boolean syncAndVerify) {
        super.apply(syncAndVerify);
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
