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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.concurrent.Lockable;
import org.cinchapi.concourse.server.concurrent.Lockables;
import org.cinchapi.concourse.server.concurrent.ConcourseExecutors;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import static com.google.common.base.Preconditions.*;
import static org.cinchapi.concourse.server.engine.Stores.*;

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
abstract class BufferedStore implements
		WritableStore,
		VersionControlStore,
		Lockable {

	private static final String threadNamePrefix = "BufferedStore";

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
		Lock lock = writeLock();
		try {
			if(!verify(key, value, record)) {
				return buffer.addUnsafe(key, value, record); /* Authorized */
			}
			return false;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Map<Long, String> audit(long record) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Map<Long, String>> bufferResult = executor
					.submit(invokeAuditCallable(buffer, record));
			Future<Map<Long, String>> storeResult = executor
					.submit(invokeAuditCallable(destination, record));
			Map<Long, String> result = storeResult.get();
			result.putAll(bufferResult.get());
			return result;

		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Map<Long, String>> bufferResult = executor
					.submit(invokeAuditCallable(buffer, key, record));
			Future<Map<Long, String>> storeResult = executor
					.submit(invokeAuditCallable(destination, key, record));
			Map<Long, String> result = storeResult.get();
			result.putAll(bufferResult.get());
			return result;

		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Set<String> describe(long record) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Set<String>> bufferResult = executor
					.submit(invokeDescribeCallable(buffer, record));
			Future<Set<String>> storeResult = executor
					.submit(invokeDescribeCallable(destination, record));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Set<String>> bufferResult = executor
					.submit(invokeDescribeCallable(buffer, record, timestamp));
			Future<Set<String>> storeResult = executor
					.submit(invokeDescribeCallable(destination, record,
							timestamp));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Set<TObject>> bufferResult = executor
					.submit(invokeFetchCallable(buffer, key, record));
			Future<Set<TObject>> storeResult = executor
					.submit(invokeFetchCallable(destination, key, record));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Set<TObject>> bufferResult = executor
					.submit(invokeFetchCallable(buffer, key, record, timestamp));
			Future<Set<TObject>> storeResult = executor
					.submit(invokeFetchCallable(destination, key, record,
							timestamp));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Set<Long>> bufferResult = executor
					.submit(invokeFindCallable(buffer, timestamp, key,
							operator, values));
			Future<Set<Long>> storeResult = executor.submit(invokeFindCallable(
					destination, timestamp, key, operator, values));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		Lock lock = readLock();
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		try {
			Future<Set<Long>> bufferResult = executor
					.submit(invokeFindCallable(buffer, key, operator, values));
			Future<Set<Long>> storeResult = executor.submit(invokeFindCallable(
					destination, key, operator, values));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public boolean ping(long record) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Boolean> bufferResult = executor.submit(invokePingCallable(
					buffer, record));
			Future<Boolean> storeResult = executor.submit(invokePingCallable(
					destination, record));
			return storeResult.get() ^ bufferResult.get();
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this);
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		Lock lock = writeLock();
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
			lock.release();
		}
	}

	@Override
	public void revert(String key, long record, long timestamp) {
		Lock lock = writeLock();
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
			lock.release();
		}

	}

	@Override
	public Set<Long> search(String key, String query) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Set<Long>> bufferResult = executor
					.submit(invokeSearchCallable(buffer, key, query));
			Future<Set<Long>> storeResult = executor
					.submit(invokeSearchCallable(destination, key, query));
			return Sets.symmetricDifference(storeResult.get(),
					bufferResult.get());
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Boolean> bufferResult = executor
					.submit(invokeVerifyCallable(buffer, key, value, record));
			Future<Boolean> storeResult = executor.submit(invokeVerifyCallable(
					destination, key, value, record));
			return storeResult.get() ^ bufferResult.get();
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		ExecutorService executor = ConcourseExecutors.newThreadPool(2,
				threadNamePrefix);
		Lock lock = readLock();
		try {
			Future<Boolean> bufferResult = executor
					.submit(invokeVerifyCallable(buffer, key, value, record,
							timestamp));
			Future<Boolean> storeResult = executor.submit(invokeVerifyCallable(
					destination, key, value, record, timestamp));
			return storeResult.get() ^ bufferResult.get();
		}
		catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
		finally {
			lock.release();
			executor.shutdown();
		}
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this);
	}

}
