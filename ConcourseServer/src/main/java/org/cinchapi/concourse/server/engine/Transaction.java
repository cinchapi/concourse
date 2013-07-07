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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Map;
import java.util.Set;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.Files;
import org.cinchapi.common.multithread.Lock;
import org.cinchapi.common.time.Time;
import org.cinchapi.concourse.server.ServerConstants;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.*;

/**
 * A server side representation of a {@link Transaction} that contains resources
 * for guaranteeing serializable isolation and durability.
 * 
 * @author jnelson
 */
public final class Transaction extends BufferingService {

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
		Transaction transaction = new Transaction(destination, Files.map(file,
				MapMode.READ_ONLY, 0, Files.length(file)));
		transaction.doCommit();
		Files.delete(file);
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

	private static final Logger log = LoggerFactory
			.getLogger(Transaction.class);

	/**
	 * The location where transaction backups are stored in order to enforce the
	 * durability guarantee.
	 */
	private static final String transactionStore = ServerConstants.DATA_HOME
			+ File.separator + "transactions";
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
	final Set<TransactionLock> locks = Sets
			.newHashSetWithExpectedSize(initialCapacity);

	/**
	 * Construct a new instance.
	 * 
	 * @param destination
	 */
	private Transaction(Engine destination) {
		super(new Limbo(initialCapacity), destination);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param destination
	 * @param bytes
	 */
	private Transaction(Engine destination, ByteBuffer bytes) {
		this(destination);
		Transactions.populateFromByteBuffer(this, bytes);
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
		locks.add((TransactionLock) destination.lockAndIsolate(key, record));
		return super.add(key, value, record);
	}

	@Override
	public Map<Long, String> audit(long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(record));
		return super.audit(record);
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key, record));
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
		Transactions.encodeAsByteBuffer(this, backup).force();
		log.info("Created backup for transaction {} at '{}'", hashCode(),
				backup);
		doCommit();
		Files.delete(backup);
		log.info("Deleted backup for transaction {} at '{}'", hashCode(),
				backup);
		return true;
	}

	@Override
	public Set<String> describe(long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(record));
		return super.describe(record);
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(record));
		return super.describe(record, timestamp);
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key, record));
		return super.fetch(key, record);
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key, record));
		return super.fetch(key, record, timestamp);
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key));
		return super.find(timestamp, key, operator, values);
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key));
		return super.find(key, operator, values);
	}

	@Override
	public boolean ping(long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(record));
		return super.ping(record);
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndIsolate(key, record));
		return super.remove(key, value, record);
	}

	@Override
	public void revert(String key, long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndIsolate(key, record));
		super.revert(key, record, timestamp);
	}

	@Override
	public Set<Long> search(String key, String query) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key));
		return super.search(key, query);
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key, record));
		return super.verify(key, value, record);
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		checkState(open, "Cannot modify a closed transaction");
		locks.add((TransactionLock) destination.lockAndShare(key, record));
		return super.verify(key, value, record, timestamp);
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
	 * Release all the locks held by this Transaction.
	 */
	private void releaseLocks() {
		for (Lock lock : locks) {
			lock.release();
		}
	}

}
