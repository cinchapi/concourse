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

/**
 * Tools used in the {@link Transaction} class.
 * 
 * @author jnelson
 */
@UtilityClass
@PackagePrivate
final class Transactions {

	private static final int LOCKS_SIZE_OFFSET = 0;
	private static final int LOCKS_SIZE_SIZE = 4;
	private static final int LOCKS_OFFSET = LOCKS_SIZE_OFFSET + LOCKS_SIZE_SIZE;

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
	public static MappedByteBuffer encodeAsByteBuffer(
			Transaction transaction, String file) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		int lockSize = 4 + (transaction.locks.size() * TransactionLock.SIZE);
		out.write(lockSize);
		out.write(transaction.locks, TransactionLock.SIZE);
		out.write(((Limbo) transaction.buffer).writes); /* Authorized */
		out.close();
		Files.open(file);
		return out.toMappedByteBuffer(file, 0);
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
			transaction.locks.add(lock);
		}

		it = ByteableCollections.iterator(writes);
		while (it.hasNext()) {
			Write write = Write.fromByteBuffer(it.next());
			((Limbo) transaction.buffer).insert(write);
		}
	}

}
