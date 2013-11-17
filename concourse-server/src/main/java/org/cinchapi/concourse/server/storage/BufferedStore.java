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

import com.beust.jcommander.internal.Maps;
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
	// TODO: Review each method and figure out if it makes sense to check the
	// destination before the buffer (i.e. the way it is done in the find()
	// methods).

	// NOTE: This class DOES NOT implement any locking directly, so it is
	// ThreadSafe if the #buffer and #destination are. Nevertheless, a
	// #masterLock is provided to subclasses in the event that they need to
	// ensure some thread safety directly.

	/**
	 * The {@code buffer} is the place where data is initially stored. The
	 * contained data is eventually moved to the {@link #destination} when the
	 * {@link ProxyStore#transport(PermanentStore)} method is called.
	 */
	protected final Limbo buffer;

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
	protected BufferedStore(Limbo transportable, PermanentStore destination) {
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
		Write write = Write.add(key, value, record);
		if(!verify(write)) {
			return buffer.insert(write); /* Authorized */
		}
		return false;
	}

	@Override
	public Map<Long, String> audit(long record) {
		Map<Long, String> result = destination.audit(record);
		result.putAll(buffer.audit(record));
		return result;
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		Map<Long, String> result = destination.audit(key, record);
		result.putAll(buffer.audit(key, record));
		return result;
	}

	@Override
	public Set<String> describe(long record) {
		Map<String, Set<TObject>> ktv = Maps.newHashMap();
		for (String key : destination.describe(record)) {
			ktv.put(key, destination.fetch(key, record));
		}
		return buffer.describeUsingContext(record, ktv);
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		Map<String, Set<TObject>> ktv = Maps.newHashMap();
		for (String key : destination.describe(record, timestamp)) {
			ktv.put(key, destination.fetch(key, record, timestamp));
		}
		return buffer.describeUsingContext(record, timestamp, ktv);
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		return Sets.symmetricDifference(buffer.fetch(key, record),
				destination.fetch(key, record));
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		return Sets.symmetricDifference(buffer.fetch(key, record, timestamp),
				destination.fetch(key, record, timestamp));
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		return Sets.symmetricDifference(
				destination.find(timestamp, key, operator, values),
				buffer.find(timestamp, key, operator, values));
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		return Sets.symmetricDifference(
				destination.find(key, operator, values),
				buffer.find(key, operator, values));
	}

	@Override
	public boolean ping(long record) {
		return buffer.ping(record) ^ destination.ping(record);
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		Write write = Write.remove(key, value, record);
		if(verify(write)) {
			return buffer.insert(write); /* Authorized */
		}
		return false;
	}

	@Override
	public void revert(String key, long record, long timestamp) {
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

	@Override
	public Set<Long> search(String key, String query) {
		return Sets.symmetricDifference(buffer.search(key, query),
				destination.search(key, query));
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		return verify(Write.notStorable(key, value, record));
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		return verify(Write.notStorable(key, value, record), timestamp);
	}

	/**
	 * Verify {@code write}.
	 * 
	 * @param write
	 * @return {@code true} if {@code write} currently exists
	 */
	private boolean verify(Write write) {
		return buffer.verify(write)
				^ destination
						.verify(write.getKey().toString(), write.getValue()
								.getTObject(), write.getRecord().longValue());
	}

	/**
	 * Verify {@code write} at {@code timestamp}.
	 * 
	 * @param write
	 * @param timestamp
	 * @return {@code true} if {@code write} exists at {@code timestamp}
	 */
	private boolean verify(Write write, long timestamp) {
		return buffer.verify(write, timestamp)
				^ destination.verify(write.getKey().toString(), write
						.getValue().getTObject(),
						write.getRecord().longValue(), timestamp);
	}

}
