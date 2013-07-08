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
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.common.annotate.DoNotInvoke;
import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.ByteBuffers;
import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.Byteables;
import org.cinchapi.common.multithread.Lock;

import com.google.common.base.Preconditions;

/**
 * A {@link Byteable} wrapper for a normal {@link Lock} that is used in a
 * {@link Transaction}. This allows the lock to be backed up and
 * reconstructed from a ByteBuffer.
 * 
 * @author jnelson
 */
@Immutable
public class TransactionLock implements Lock, Byteable {

	/**
	 * Return the {@code TransactionLock} that is encoded in {@code bytes}.
	 * 
	 * @param bytes
	 * @return the Lock
	 */
	public static TransactionLock fromByteBuffer(ByteBuffer bytes) {
		return Byteables.read(bytes, TransactionLock.class);
	}

	/**
	 * Return a lock that conforms to
	 * {@link Isolatable#lockAndIsolate(String, long)}.
	 * 
	 * @param key
	 * @param record
	 * @return the Lock
	 */
	public static TransactionLock lockAndIsolate(String key, long record) {
		return new TransactionLock(Representation.forObjects(key, record),
				Type.ISOLATED_FIELD);
	}

	/**
	 * Return a lock that conforms to {@link Isolatable#lockAndShare(long)}.
	 * 
	 * @param record
	 * @return the Lock
	 */
	public static TransactionLock lockandShare(long record) {
		return new TransactionLock(Representation.forObjects(record),
				Type.SHARED_RECORD);
	}

	/**
	 * Return a lock that conforms to {@link Isolatable#lockAndShare(String)}.
	 * 
	 * @param key
	 * @return the Lock
	 */
	public static TransactionLock lockAndShare(String key) {
		return new TransactionLock(Representation.forObjects(key),
				Type.SHARED_KEY);
	}

	/**
	 * Return a lock that conforms to
	 * {@link Isolatable#lockAndShare(String, long)}.
	 * 
	 * @param key
	 * @param record
	 * @return the Lock
	 */
	public static TransactionLock lockAndShare(String key, long record) {
		return new TransactionLock(Representation.forObjects(key, record),
				Type.SHARED_FIELD);
	}

	private static final int SOURCE_OFFSET = 0;
	private static final int SOURCE_SIZE = 16;
	private static final int TYPE_OFFSET = SOURCE_OFFSET + SOURCE_SIZE;
	private static final int TYPE_SIZE = 4;

	@PackagePrivate
	static final int SIZE = SOURCE_SIZE + TYPE_SIZE;

	/**
	 * The actual lock/release functionality is delegated to this object.
	 */
	private final transient Lock lock;

	/**
	 * A Representation is used to refer to the locked resource before and after
	 * serialization.
	 */
	private final Representation source;

	/**
	 * The Type ensures that the same level of isolation is maintained before
	 * and after serialization.
	 */
	private final Type type;

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	@DoNotInvoke
	public TransactionLock(ByteBuffer bytes) {
		Preconditions.checkArgument(bytes.capacity() == SIZE);
		this.source = Representation.fromByteBuffer(ByteBuffers.slice(bytes,
				SOURCE_OFFSET, SOURCE_SIZE));
		this.type = ByteBuffers.getEnum(
				ByteBuffers.slice(bytes, TYPE_OFFSET, TYPE_SIZE), Type.class);
		this.lock = type == Type.ISOLATED_FIELD ? this.source.writeLock()
				: this.source.readLock();
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param source
	 * @param type
	 */
	private TransactionLock(Representation source, Type type) {
		this.source = source;
		this.type = type;
		this.lock = type == Type.ISOLATED_FIELD ? this.source.writeLock()
				: this.source.readLock();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TransactionLock) {
			TransactionLock other = (TransactionLock) obj;
			return source.equals(other.source) && type.equals(other.type);
		}
		return false;
	}

	@Override
	public ByteBuffer getBytes() {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		out.write(source);
		out.write(type);
		out.close();
		return out.toByteBuffer();
	}

	@Override
	public int hashCode() {
		return Objects.hash(source, type);
	}

	@Override
	public void release() {
		lock.release();
	}

	@Override
	public int size() {
		return SIZE;
	}

	/**
	 * The types of TransactionLocks.
	 * 
	 * @author jnelson
	 */
	private enum Type {
		SHARED_RECORD, SHARED_KEY, SHARED_FIELD, ISOLATED_FIELD
	}

}
