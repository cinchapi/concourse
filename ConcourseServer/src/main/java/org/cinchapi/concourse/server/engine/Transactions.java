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

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.annotate.UtilityClass;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.ByteableCollections;
import org.cinchapi.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tools used in the {@link Transaction} class.
 * 
 * @author jnelson
 */
@UtilityClass
@PackagePrivate
final class Transactions {

	private static final Logger log = LoggerFactory
			.getLogger(Transaction.class);

	/**
	 * Encode {@code transaction} as a MappedByteBuffer with the following
	 * format:
	 * <ol>
	 * <li><strong>locksSize</strong> - position {@value #LOCKS_SIZE_OFFSET}</li>
	 * <li><strong>locks</strong> - position {@value #LOCKS_OFFSET}</li>
	 * <li><strong>writes</strong> - position {@value #LOCKS_OFFSET} + locksSize
	 * </li>
	 * </ol>
	 * 
	 * @param transaction
	 * @param file
	 * @return the encoded ByteBuffer
	 */
	public static MappedByteBuffer encodeAsByteBuffer(Transaction transaction,
			String file) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		int lockSize = 4 + (transaction.locks.size() * TransactionLock.SIZE);
		out.write(lockSize);
		out.write(transaction.locks.values(), TransactionLock.SIZE);
		out.write(((Limbo) transaction.buffer).writes); /* Authorized */
		out.close();
		Files.open(file);
		return out.toMappedByteBuffer(file, 0);
	}

	/**
	 * Grab an exclusive lock on the field identified by {@code key} in
	 * {@code record}.
	 * 
	 * @param key
	 * @param record
	 */
	public static void lockAndIsolate(Transaction transaction, String key,
			long record) {
		lock(transaction, Representation.forObjects(key, record),
				TransactionLock.Type.ISOLATED_FIELD);
	}

	/**
	 * Grab a shared lock on {@code record}.
	 * 
	 * @param transaction
	 * @param record
	 */
	public static void lockAndShare(Transaction transaction, long record) {
		lock(transaction, Representation.forObjects(record),
				TransactionLock.Type.SHARED_RECORD);
	}

	/**
	 * Grab a shared lock for {@code key}.
	 * 
	 * @param transaction
	 * @param key
	 */
	public static void lockAndShare(Transaction transaction, String key) {
		lock(transaction, Representation.forObjects(key),
				TransactionLock.Type.SHARED_KEY);
	}

	/**
	 * Grab a shared lock on the field identified by {@code key} in
	 * {@code record}.
	 * 
	 * @param transaction
	 * @param key
	 * @param record
	 */
	public static void lockAndShare(Transaction transaction, String key,
			long record) {
		lock(transaction, Representation.forObjects(key, record),
				TransactionLock.Type.SHARED_FIELD);
	}

	/**
	 * Populate {@code transaction} with the data encoded in {@code bytes}. This
	 * method assumes that {@code transaction} is empty.
	 * 
	 * @param serverTransaction
	 * @param bytes
	 */
	public static void populateFromByteBuffer(Transaction transaction,
			ByteBuffer bytes) {
		int locksSize = bytes.getInt();
		int writesPosition = LOCKS_OFFSET + locksSize;
		int writesSize = bytes.capacity() - writesPosition;
		ByteBuffer locks = ByteBuffers.slice(bytes, LOCKS_OFFSET, locksSize);
		ByteBuffer writes = ByteBuffers
				.slice(bytes, writesPosition, writesSize);

		Iterator<ByteBuffer> it = ByteableCollections.iterator(locks,
				TransactionLock.SIZE);
		while (it.hasNext()) {
			TransactionLock lock = TransactionLock.fromByteBuffer(it.next());
			transaction.locks.put(lock.getSource(), lock);
		}

		it = ByteableCollections.iterator(writes);
		while (it.hasNext()) {
			Write write = Write.fromByteBuffer(it.next());
			((Limbo) transaction.buffer).insert(write);
		}
	}

	/**
	 * Grab a lock of {@code type} for {@code representation} in
	 * {@code transaction}.
	 * 
	 * @param transaction
	 * @param representation
	 * @param type
	 */
	private static void lock(Transaction transaction,
			Representation representation, TransactionLock.Type type) {
		if(transaction.locks.containsKey(representation)
				&& transaction.locks.get(representation).getType() == TransactionLock.Type.SHARED_FIELD
				&& type == TransactionLock.Type.ISOLATED_FIELD) {
			// Lock "upgrades" should only occur in the event that we previously
			// held a shared field lock and now we need an isolated field lock
			// (i.e we were reading a field and now we want to write to that
			// field). It is technically, not possible to upgrade a read lock to
			// a write lock, so we must first release the read lock and grab a
			// new write lock.
			transaction.locks.remove(representation).release();
			log.debug("Removed shared field lock for representation {} "
					+ "in transaction {}", representation, transaction);
		}
		if(!transaction.locks.containsKey(representation)) {
			transaction.locks.put(representation, new TransactionLock(
					representation, type));
			log.debug("Grabbed {} lock for representation {} "
					+ "in transaction {}", type, representation, transaction);
		}

	}

	private static final int LOCKS_SIZE_OFFSET = 0;

	private static final int LOCKS_SIZE_SIZE = 4;

	private static final int LOCKS_OFFSET = LOCKS_SIZE_OFFSET + LOCKS_SIZE_SIZE;

}
