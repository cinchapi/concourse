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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.annotate.Restricted;
import org.cinchapi.concourse.server.concurrent.Token;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.temp.Queue;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An {@link AtomicOperation} that performs backups prior to commit to make sure
 * that it is durable in the event of crash, power loss or failure.
 * 
 * @author jnelson
 */
public final class Transaction extends AtomicOperation implements Compoundable {

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
     * a given token.
     */
    @SuppressWarnings("serial")
    private final Map<Token, Set<VersionChangeListener>> versionChangeListeners = new HashMap<Token, Set<VersionChangeListener>>() {

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
        open = false;
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
    public boolean add(String key, TObject value, long record)
            throws AtomicStateException {
        if(super.add(key, value, record)) {
            notifyVersionChange(Token.wrap(key, record));
            notifyVersionChange(Token.wrap(record));
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public boolean remove(String key, TObject value, long record)
            throws AtomicStateException {
        if(super.remove(key, value, record)) {
            notifyVersionChange(Token.wrap(key, record));
            notifyVersionChange(Token.wrap(record));
            return true;
        }
        else {
            return false;
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
        ((Engine) destination).addVersionChangeListener(token, listener);
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
        for (VersionChangeListener listener : versionChangeListeners.get(token)) {
            listener.onVersionChange(token);
        }
    }

    @Override
    @Restricted
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener) {
        versionChangeListeners.get(token).remove(listener);
        ((Engine) destination).removeVersionChangeListener(token, listener);
    }

    @Override
    public AtomicOperation startAtomicOperation() {
        return AtomicOperation.start(this);
    }

    @Override
    public String toString() {
        return id;
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
            LockDescription lock = LockDescription.fromByteBuffer(it.next());
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

}
