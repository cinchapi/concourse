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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.cinchapi.common.util.NonBlockingHashMultimap;
import org.cinchapi.common.util.NonBlockingRangeMap;
import org.cinchapi.common.util.Range;
import org.cinchapi.common.util.RangeMap;
import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.concurrent.LockService;
import org.cinchapi.concourse.server.concurrent.RangeLockService;
import org.cinchapi.concourse.server.concurrent.RangeToken;
import org.cinchapi.concourse.server.concurrent.RangeTokens;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.temp.Queue;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * An {@link AtomicOperation} that performs backups prior to commit to make sure
 * that it is durable in the event of crash, power loss or failure.
 * 
 * @author jnelson
 */
public final class Transaction extends AtomicOperation implements Compoundable {
    // NOTE: Because Transaction's rely on JIT locking, the safe methods are
    // identical to the unsafe ones and do not grab any locks
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
            transaction.invokeSuperDoCommit();
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
     * The unique Transaction id.
     */
    private final String id;

    /**
     * A collection of listeners that should be notified of a version change for
     * a given range token.
     */
    private final RangeMap<Value, VersionChangeListener> rangeVersionChangeListeners = NonBlockingRangeMap
            .create();

    /**
     * A collection of listeners that should be notified of a version change for
     * a given token.
     */
    private final Multimap<Token, VersionChangeListener> versionChangeListeners = NonBlockingHashMultimap
            .create();

    /**
     * Construct a new instance.
     * 
     * @param destination
     */
    private Transaction(Engine destination) {
        super(destination);
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
        ((Compoundable) destination).addVersionChangeListener(token, this);
        // This rest of this implementation is unnecessary since Transactions
        // are assumed to
        // be isolated (e.g. single-threaded), but is kept here for unit test
        // consistency.
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
    public long getVersion(long record) {
        return Math.max(buffer.getVersion(record),
                ((Engine) destination).getVersion(record));
    }

    @Override
    public long getVersion(String key) {
        return Math.max(buffer.getVersion(key),
                ((Engine) destination).getVersion(key));
    }

    @Override
    public long getVersion(String key, long record) {
        return Math.max(buffer.getVersion(key, record),
                ((Engine) destination).getVersion(key, record));
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
    @Restricted
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener) {
        // This implementation is unnecessary since Transactions are assumed to
        // be isolated (e.g. single-threaded), but is kept here for consistency.
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
    public Map<Long, String> auditUnsafe(long record) {
        return audit(record);
    }

    @Override
    public Map<Long, String> auditUnsafe(String key, long record) {
        return audit(key, record);
    }

    @Override
    public Map<String, Set<TObject>> browseUnsafe(long record) {
        return browse(record);
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
    public Set<TObject> fetchUnsafe(String key, long record) {
        return fetch(key, record);
    }

    @Override
    public boolean verifyUnsafe(String key, TObject value, long record) {
        return verify(key, value, record);
    }

    @Override
    public AtomicOperation startAtomicOperation() {
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
     * performing a backup (i.e. when restoring from a backup).
     */
    private void invokeSuperDoCommit() {
        super.doCommit();
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
        String file = ((Engine) destination).transactionStore + File.separator
                + id + ".txn";
        FileChannel channel = FileSystem.getFileChannel(file);
        try {
            channel.write(serialize());
            channel.force(true);
            Logger.info("Created backup for transaction {} at '{}'", this, file);
            invokeSuperDoCommit();
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
