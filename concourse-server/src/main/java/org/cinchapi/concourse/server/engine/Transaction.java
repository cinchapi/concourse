/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.engine;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.io.ByteBufferOutputStream;
import org.cinchapi.concourse.server.io.ByteableCollections;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.model.Write;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.slf4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.*;
import static org.cinchapi.concourse.util.Loggers.getLogger;

/**
 * A server side representation of a {@link Transaction} that contains resources
 * for guaranteeing serializable isolation and durability.
 * 
 * @author jnelson
 */
public final class Transaction extends BufferedStore {

	/**
	 * Return the Transaction for {@code destination} that is backed up to
	 * {@code file}. This method will finish committing the transaction before
	 * returning.
	 * 
	 * @param destination
	 * @param file
	 * @return The restored ServerTransaction
	 */
	public static Transaction restore(Engine destination, String file) {
		Transaction transaction = new Transaction(destination, FileSystem.map(
				file, MapMode.READ_ONLY, 0, FileSystem.getFileSize(file)));
		transaction.doCommit();
		FileSystem.deleteFile(file);
		return transaction;
	}

	/**
	 * Return a new Transaction with {@code engine} as the eventual destination.
	 * 
	 * @param engine
	 * @return the new ServerTransaction
	 */
	public static Transaction start(Engine engine) {
		return new Transaction(engine);
	}

	private static final Logger log = getLogger();

	/**
	 * The location where transaction backups are stored in order to enforce the
	 * durability guarantee.
	 */
	private final String transactionStore;

	/**
	 * The initial size of the memory {@link Queue}.
	 */
	private static final int initialCapacity = 50;

	/**
	 * The Transaction is open so long as it has not been committed or aborted.
	 */
	private boolean open = true;

	/**
	 * The Transaction acquires a collection of shared and exclusive locks to
	 * enforce the serializable isolation guarantee.
	 */
	@PackagePrivate
	final Map<Representation, TransactionLock> locks = Maps
			.newHashMapWithExpectedSize(initialCapacity);

	/**
	 * Construct a new instance.
	 * 
	 * @param destination
	 */
	private Transaction(Engine destination) {
		super(new Queue(initialCapacity), destination);
		this.transactionStore = destination.bufferStore + File.separator
				+ "transactions";
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param destination
	 * @param bytes
	 */
	private Transaction(Engine destination, ByteBuffer bytes) {
		this(destination);
		int locksSize = bytes.getInt();
		int writesPosition = 4 + locksSize;
		int writesSize = bytes.capacity() - writesPosition;
		ByteBuffer locks = ByteBuffers.slice(bytes, 4, locksSize);
		ByteBuffer writes = ByteBuffers
				.slice(bytes, writesPosition, writesSize);

		Iterator<ByteBuffer> it = ByteableCollections.iterator(locks,
				TransactionLock.SIZE);
		while (it.hasNext()) {
			TransactionLock lock = TransactionLock.fromByteBuffer(it.next());
			this.locks.put(lock.getSource(), lock);
		}

		it = ByteableCollections.iterator(writes);
		while (it.hasNext()) {
			Write write = Write.fromByteBuffer(it.next());
			((Queue) buffer).insert(write);
		}
		open = false;
	}

	/**
	 * Discard the Transaction and all of its changes. Once a Transaction is
	 * aborted, it cannot process further requests or be committed.
	 */
	public void abort() {
		open = false;
		releaseLocks();
		log.info("Aborted transaction {}", hashCode());
	}

	@Override
	public boolean add(String key, TObject value, long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndIsolate(key, record);
		return super.add(key, value, record);
	}

	@Override
	public Map<Long, String> audit(long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(record);
		return super.audit(record);
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key, record);
		return super.audit(key, record);
	}

	/**
	 * Commit the changes in the Transaction to the database. This
	 * operation will succeed if and only if all the contained reads/writes are
	 * successfully applied to the current state of the database.
	 * 
	 * @return {@code true} if the Transaction was successfully committed
	 */
	public boolean commit() {
		checkState(open, "Cannot commit a closed transaction");
		open = false;
		String backup = transactionStore + File.separator + Time.now() + ".txn";
		backup(backup);
		doCommit();
		FileSystem.deleteFile(backup);
		log.info("Deleted backup for transaction {} at '{}'", hashCode(),
				backup);
		return true;
	}

	@Override
	public Set<String> describe(long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(record);
		return super.describe(record);
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(record);
		return super.describe(record, timestamp);
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key, record);
		return super.fetch(key, record);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key, record);
		return super.fetch(key, record, timestamp);
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key);
		return super.find(timestamp, key, operator, values);
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key);
		return super.find(key, operator, values);
	}

	@Override
	public boolean ping(long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(record);
		return super.ping(record);
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndIsolate(key, record);
		return super.remove(key, value, record);
	}

	@Override
	public void revert(String key, long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndIsolate(key, record);
		super.revert(key, record, timestamp);
	}

	@Override
	public Set<Long> search(String key, String query) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key);
		return super.search(key, query);
	}

	@Override
	public void start() {
		// do nothing
	}

	@Override
	public String toString() {
		return Integer.toString(hashCode());
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key, record);
		return super.verify(key, value, record);
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		lockAndShare(key, record);
		return super.verify(key, value, record, timestamp);
	}

	/**
	 * Backup this transaction to {@code file}.
	 * 
	 * @param file
	 */
	private void backup(String file) {
		FileChannel channel = FileSystem.getFileChannel(file);
		try {
			channel.write(getBytes());
			log.info("Created backup for transaction {} at '{}'", hashCode(),
					file);
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
		finally {
			FileSystem.closeFileChannel(channel);
		}
	}

	/**
	 * Transport the writes to {@code destination} and release the held locks.
	 */
	private void doCommit() {
		log.info("Starting commit for transaction {}", hashCode());
		buffer.transport(destination);
		releaseLocks();
		log.info("Finished commit for transaction {}", hashCode());
	}

	/**
	 * Encode this transaction as a ByteBuffer with the following
	 * format:
	 * <ol>
	 * <li><strong>locksSize</strong> - position
	 * {@value Transaction#LOCKS_SIZE_OFFSET}</li>
	 * <li><strong>locks</strong> - position {@value Transaction#LOCKS_OFFSET}</li>
	 * <li><strong>writes</strong> - position {@value Transaction#LOCKS_OFFSET}
	 * + locksSize</li>
	 * </ol>
	 * 
	 * @param transaction
	 * @param file
	 * @return the encoded ByteBuffer
	 */
	private ByteBuffer getBytes() {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		int lockSize = 4 + (locks.size() * TransactionLock.SIZE);
		out.write(lockSize);
		out.write(locks.values(), TransactionLock.SIZE);
		out.write(((Queue) buffer).writes); /* Authorized */
		out.close();
		return out.toByteBuffer();
	}

	/**
	 * Grab a lock of {@code type} for {@code representation} in this.
	 * transaction
	 * 
	 * @param representation
	 * @param type
	 */
	private void lock(Representation representation, TransactionLock.Type type) {
		if(locks.containsKey(representation)
				&& locks.get(representation).getType() == TransactionLock.Type.SHARED_FIELD
				&& type == TransactionLock.Type.ISOLATED_FIELD) {
			// Lock "upgrades" should only occur in the event that we previously
			// held a shared field lock and now we need an isolated field lock
			// (i.e we were reading a field and now we want to write to that
			// field). It is technically, not possible to upgrade a read lock to
			// a write lock, so we must first release the read lock and grab a
			// new write lock.
			locks.remove(representation).release();
			log.debug("Removed {} lock for representation {} "
					+ "in transaction {}", TransactionLock.Type.SHARED_FIELD,
					representation, this);
		}
		if(!locks.containsKey(representation)) {
			locks.put(representation, new TransactionLock(representation, type));
			log.debug("Grabbed {} lock for representation {} "
					+ "in transaction {}", type, representation, this);
		}
	}

	/**
	 * Grab an exclusive lock on the field identified by {@code key} in
	 * {@code record}.
	 * 
	 * @param key
	 * @param record
	 */
	private void lockAndIsolate(String key, long record) {
		lock(Representation.forObjects(key, record),
				TransactionLock.Type.ISOLATED_FIELD);
	}

	/**
	 * Grab a shared lock on {@code record}.
	 * 
	 * @param record
	 */
	private void lockAndShare(long record) {
		lock(Representation.forObjects(record),
				TransactionLock.Type.SHARED_RECORD);
	}

	/**
	 * Grab a shared lock for {@code key}.
	 * 
	 * @param key
	 */
	private void lockAndShare(String key) {
		lock(Representation.forObjects(key), TransactionLock.Type.SHARED_KEY);
	}

	/**
	 * Grab a shared lock on the field identified by {@code key} in
	 * {@code record}.
	 * 
	 * @param key
	 * @param record
	 */
	private void lockAndShare(String key, long record) {
		lock(Representation.forObjects(key, record),
				TransactionLock.Type.SHARED_FIELD);
	}

	/**
	 * Release all the locks held by this Transaction.
	 */
	private void releaseLocks() {
		for (Lock lock : locks.values()) {
			lock.release();
		}
	}

}
