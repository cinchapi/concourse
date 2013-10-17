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
package org.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

import com.google.common.collect.Sets;
import static com.google.common.base.Preconditions.*;

/**
 * A {@link BufferedStore} holds data in a {@link ProxyStore} buffer before
 * making batch commits to some other {@link PermanentStore}.
 * <p>
 * Data is written to the buffer until the buffer is full, at which point the
 * BufferingStore will flush the data to the destination store. Reads are
 * handled by taking the XOR (see {@link Sets#symmetricDifference(Set, Set)} or
 * XOR truth (see <a
 * href="http://en.wikipedia.org/wiki/Exclusive_or#Truth_table"
 * >http://en.wikipedia.org/wiki/Exclusive_or#Truth_table</a>) of the values
 * read from the buffer and the destination.
 * </p>
 * 
 * @author jnelson
 */
@PackagePrivate
@ThreadSafe
abstract class BufferedStore implements WritableStore, VersionControlStore {

	/**
	 * The {@code buffer} is the place where data is initially stored. The
	 * contained data is eventually moved to the {@link #destination} when the
	 * {@link ProxyStore#transport(PermanentStore)} method is called.
	 */
	protected final ProxyStore buffer;

	/**
	 * The {@code destination} is the place where data is stored when it is
	 * transferred from the {@link #buffer}. The {@code destination} defines its
	 * protocol for accepting data in the {@link PermanentStore#accept(Write)}
	 * method.
	 */
	protected final PermanentStore destination;

	/**
	 * Lock used to ensure the object is ThreadSafe. This lock provides access
	 * to a masterLock.readLock()() and masterLock.writeLock()().
	 */
	protected final ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

	/**
	 * Construct a new instance.
	 * 
	 * @param transportable
	 * @param destination
	 */
	protected BufferedStore(ProxyStore transportable, PermanentStore destination) {
		checkArgument(
				!this.getClass().isAssignableFrom(destination.getClass()),
				"Cannot embed a %s into %s", destination.getClass(),
				this.getClass());
		checkArgument(
				!this.getClass().isAssignableFrom(transportable.getClass()),
				"Cannot embed a %s into %s", transportable.getClass(),
				this.getClass());
		this.buffer = transportable;
		this.destination = destination;
	}

	@Override
	public boolean add(String key, TObject value, long record) {
		masterLock.writeLock().lock();
		try {
			if(!verify(key, value, record)) {
				return buffer.addUnsafe(key, value, record); /* Authorized */
			}
			return false;
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	@Override
	public Map<Long, String> audit(long record) {
		masterLock.readLock().lock();
		try {
			Map<Long, String> result = buffer.audit(record);
			result.putAll(destination.audit(record));
			return result;

		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		masterLock.readLock().lock();
		try {
			Map<Long, String> result = buffer.audit(key, record);
			result.putAll(destination.audit(key, record));
			return result;
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Set<String> describe(long record) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(buffer.describe(record),
					destination.describe(record));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(buffer.describe(record, timestamp),
					destination.describe(record, timestamp));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(buffer.fetch(key, record),
					destination.fetch(key, record));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(
					buffer.fetch(key, record, timestamp),
					destination.fetch(key, record, timestamp));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(
					buffer.find(timestamp, key, operator, values),
					destination.find(timestamp, key, operator, values));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(buffer.find(key, operator, values),
					destination.find(key, operator, values));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public boolean ping(long record) {
		masterLock.readLock().lock();
		try {
			return buffer.ping(record) ^ destination.ping(record);
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		masterLock.writeLock().lock();
		try {
			if(verify(key, value, record)) {
				return buffer.removeUnsafe(key, value, record); /* Authorized */
			}
			return false;
		}
		catch (BufferCapacityException e) {
			buffer.transport(destination);
			return remove(key, value, record);
		}
		finally {
			masterLock.writeLock().unlock();
		}
	}

	@Override
	public void revert(String key, long record, long timestamp) {
		masterLock.writeLock().lock();
		try {
			Set<TObject> past = fetch(key, record, timestamp);
			Set<TObject> present = fetch(key, record);
			Set<TObject> xor = Sets.symmetricDifference(past, present);
			for (TObject value : xor) {
				if(present.contains(value)) {
					remove(key, value, record);
				}
				else {
					add(key, value, record);
				}
			}
		}
		finally {
			masterLock.writeLock().unlock();
		}

	}

	@Override
	public Set<Long> search(String key, String query) {
		masterLock.readLock().lock();
		try {
			return Sets.symmetricDifference(buffer.search(key, query),
					destination.search(key, query));
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		masterLock.readLock().lock();
		try {
			return buffer.verify(key, value, record)
					^ destination.verify(key, value, record);
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		masterLock.readLock().lock();
		try {
			return buffer.verify(key, value, record, timestamp)
					^ destination.verify(key, value, record, timestamp);
		}
		finally {
			masterLock.readLock().unlock();
		}
	}

}
