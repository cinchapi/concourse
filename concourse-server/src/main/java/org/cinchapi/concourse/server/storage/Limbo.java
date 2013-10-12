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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.StringTools;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.server.concurrent.Lock;
import org.cinchapi.concourse.server.concurrent.Lockable;
import org.cinchapi.concourse.server.concurrent.Lockables;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;
import org.cinchapi.concourse.time.Time;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * {@link Limbo} is a lightweight in-memory {@link ProxyStore} that
 * is a suitable cache or fast, albeit temporary, store for data that will
 * eventually be persisted to disk.
 * <p>
 * The store is designed to write data very quickly <strong>
 * <em>at the expense of much slower read time.</em></strong> {@code Limbo} does
 * not index<sup>1</sup> any of the data it stores, so reads are not as
 * efficient as they would normally be in the {@link Database}.
 * </p>
 * <sup>1</sup> - All reads are O(n) because {@code Limbo} uses an
 * {@link #iterator()} to traverse the {@link Write} objects that it stores.
 * 
 * @author jnelson
 */
@ThreadSafe
@PackagePrivate
abstract class Limbo implements Lockable, ProxyStore, Iterable<Write> {

	/**
	 * A Predicate that is used to filter out empty sets.
	 */
	private static final Predicate<Set<Value>> emptySetFilter = new Predicate<Set<Value>>() {

		@Override
		public boolean apply(@Nullable Set<Value> input) {
			return !input.isEmpty();
		}

	};

	@Override
	public boolean add(String key, TObject value, long record) {
		Lock lock = writeLock();
		try {
			return !verify(key, value, record) ? addUnsafe(key, value, record)
					: false;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public boolean addUnsafe(String key, TObject value, long record) {
		return insert(Write.add(key, value, record));
	}

	@Override
	public Map<Long, String> audit(long record) {
		Lock lock = readLock();
		try {
			Map<Long, String> audit = Maps.newTreeMap();
			Iterator<Write> it = iterator();
			while (it.hasNext()) {
				Write write = it.next();
				if(write.getRecord().longValue() == record) {
					audit.put(write.getVersion(), write.toString());
				}
			}
			return audit;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Map<Long, String> audit(String key, long record) {
		Lock lock = readLock();
		try {
			Map<Long, String> audit = Maps.newTreeMap();
			Iterator<Write> it = iterator();
			while (it.hasNext()) {
				Write write = it.next();
				if(write.getKey().toString().equals(key)
						&& write.getRecord().longValue() == record) {
					audit.put(write.getVersion(), write.toString());
				}
			}
			return audit;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Set<String> describe(long record) {
		return describe(record, Time.now());
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		Lock lock = readLock();
		try {
			Map<String, Set<Value>> ktv = Maps.newHashMap();
			Iterator<Write> it = iterator();
			search: while (it.hasNext()) {
				Write write = it.next();
				if(write.getRecord().longValue() == record) {
					if(write.getVersion() <= timestamp) {
						Set<Value> values = Sets.newHashSet();
						values = ktv.get(write.getKey().toString());
						if(values == null) {
							values = Sets.newHashSet();
							ktv.put(write.getKey().toString(), values);
						}
						if(values.contains(write.getValue())) {
							values.remove(write.getValue());
						}
						else {
							values.add(write.getValue());
						}
					}
					else {
						break search;
					}
				}
			}
			return Maps.filterValues(ktv, emptySetFilter).keySet();

		}
		finally {
			lock.release();
		}
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		return fetch(key, record, Time.now());
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		Lock lock = readLock();
		try {
			Set<TObject> values = Sets.newLinkedHashSet();
			Iterator<Write> it = iterator();
			while (it.hasNext()) {
				Write write = it.next();
				if(write.getVersion() <= timestamp) {
					if(key.equals(write.getKey().toString())
							&& Longs.compare(record, write.getRecord()
									.longValue()) == 0) {
						if(values.contains(write.getValue().getTObject())) {
							values.remove(write.getValue().getTObject());
						}
						else {
							values.add(write.getValue().getTObject());
						}
					}
				}
				else {
					break;
				}
			}
			return values;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
		Lock lock = readLock();
		try {
			Map<Long, Set<Value>> rtv = Maps.newLinkedHashMap();
			Iterator<Write> it = iterator();
			Value value = Value.wrap(values[0]);
			while (it.hasNext()) {
				Write write = it.next();
				long record = write.getRecord().longValue();
				Value writeValue = write.getValue();
				if(write.getVersion() < timestamp) {
					boolean matches = false;
					if(write.getKey().toString().equals(key)) {
						if(operator == Operator.EQUALS) {
							matches = value.equals(writeValue);
						}
						else if(operator == Operator.NOT_EQUALS) {
							matches = !value.equals(writeValue);
						}
						else if(operator == Operator.GREATER_THAN) {
							matches = value.compareTo(writeValue) < 0;
						}
						else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
							matches = value.compareTo(writeValue) <= 0;
						}
						else if(operator == Operator.LESS_THAN) {
							matches = value.compareTo(writeValue) > 0;
						}
						else if(operator == Operator.LESS_THAN_OR_EQUALS) {
							matches = value.compareTo(writeValue) >= 0;
						}
						else if(operator == Operator.BETWEEN) {
							Preconditions.checkArgument(values.length > 1);
							Value value2 = Value.wrap(values[1]);
							matches = value.compareTo(writeValue) <= 0
									&& value2.compareTo(write.getValue()) > 0;

						}
						else if(operator == Operator.REGEX) {
							matches = writeValue.getObject().toString()
									.matches(value.getObject().toString());
						}
						else if(operator == Operator.NOT_REGEX) {
							matches = !writeValue.getObject().toString()
									.matches(value.getObject().toString());
						}
						else {
							throw new UnsupportedOperationException();
						}
					}
					if(matches) {
						Set<Value> v = rtv.get(record);
						if(v == null) {
							v = Sets.newHashSet();
							rtv.put(record, v);
						}
						if(v.contains(writeValue)) {
							v.remove(writeValue);
						}
						else {
							v.add(writeValue);
						}
					}
				}
				else {
					break;
				}
			}
			return Maps.filterValues(rtv, emptySetFilter).keySet();
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Set<Long> find(String key, Operator operator, TObject... values) {
		return find(Time.now(), key, operator, values);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * <strong>NOTE:</strong> The subclass <em>may</em> override this method to
	 * provide an iterator with granular locking functionality for increased
	 * throughput.
	 * </p>
	 */
	@Override
	public abstract Iterator<Write> iterator();

	@Override
	public boolean ping(long record) {
		return !describe(record).isEmpty();
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this);
	}

	@Override
	public boolean remove(String key, TObject value, long record) {
		Lock lock = writeLock();
		try {
			return verify(key, value, record) ? removeUnsafe(key, value, record)
					: false;
		}
		finally {
			lock.release();
		}
	}

	@Override
	public boolean removeUnsafe(String key, TObject value, long record) {
		return insert(Write.remove(key, value, record));
	}

	@Override
	public Set<Long> search(String key, String query) {
		Lock lock = readLock();
		try {
			Map<Long, Set<Value>> rtv = Maps.newHashMap();
			Iterator<Write> it = iterator();
			while (it.hasNext()) {
				Write write = it.next();
				Value value = write.getValue();
				long record = write.getRecord().longValue();
				if(value.getType() == Type.STRING) {
					String stored = StringTools.stripStopWords((String) (value
							.getObject()));
					query = StringTools.stripStopWords(query);
					if(!Strings.isNullOrEmpty(stored)
							&& !Strings.isNullOrEmpty(query)
							&& stored.contains(query)) {
						Set<Value> values = rtv.get(record);
						if(values == null) {
							values = Sets.newHashSet();
							rtv.put(record, values);
						}
						if(values.contains(value)) {
							values.remove(value);
						}
						else {
							values.add(value);
						}

					}
				}
			}
			return Maps.filterValues(rtv, emptySetFilter).keySet();
		}
		finally {
			lock.release();
		}
	}

	@Override
	public void transport(PermanentStore destination) {
		Lock lock = writeLock();
		try {
			Iterator<Write> it = iterator();
			while (it.hasNext()) {
				destination.accept(it.next());
				it.remove();
			}
		}
		finally {
			lock.release();
		}
	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		return verify(key, value, record, Time.now());
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		Lock lock = readLock();
		try {
			Write comp = Write.notStorable(key, value, record);
			int count = 0;
			Iterator<Write> it = iterator();
			while (it.hasNext()) {
				Write write = it.next();
				if(write.getVersion() <= timestamp) {
					if(write.equals(comp)) {
						count++;
					}
				}
				else {
					break;
				}
			}
			return Numbers.isOdd(count);
		}
		finally {
			lock.release();
		}
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this);
	}

	/**
	 * Insert {@code write} into the store. This method exists so that a
	 * subclass can override the {@code add()} and {@code remove()} methods to
	 * do additional things (i.e. the case when the {@link Buffer} copies the
	 * revision to disk for durability) whilst using the same Write with the
	 * same timestamp. In these cases, the overridden method should call this
	 * method once it is ready to store {@code write} in memory.
	 * <p>
	 * <em><strong>WARNING:</strong> This method does not verify that {@code write}
	 * is a legal argument (i.e. when trying to add data there is no check done
	 * here to ensure that the data does not already exist). Those checks must
	 * be performed by the caller prior to invoking this function.</em>
	 * </p>
	 * 
	 * @param write
	 * @return {@code true} if the {@code write} is inserted into the store.
	 */
	protected abstract boolean insert(Write write);

}
