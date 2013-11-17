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
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.cinchapi.concourse.util.StringTools;
import org.cinchapi.concourse.annotate.PackagePrivate;
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
 * {@link Limbo} is a lightweight in-memory proxy store that
 * is a suitable cache or fast, albeit temporary, store for data that will
 * eventually be persisted to a {@link PermanentStore}.
 * <p>
 * The store is designed to write data very quickly <strong>
 * <em>at the expense of much slower read time.</em></strong> {@code Limbo} does
 * not index<sup>1</sup> any of the data it stores, so reads are not as
 * efficient as they would normally be in the {@link Database}.
 * </p>
 * <p>
 * This class provides naive read implementations for the methods specified in
 * the {@link WritableStore} interface, but the subclass is free to override
 * those methods to provide smarter implementations of introduce concurrency
 * controls.
 * </p>
 * <sup>1</sup> - All reads are O(n) because {@code Limbo} uses an
 * {@link #iterator()} to traverse the {@link Write} objects that it stores.
 * 
 * @author jnelson
 */
@NotThreadSafe
@PackagePrivate
abstract class Limbo implements WritableStore, Iterable<Write> {

	/**
	 * The writeLock ensures that only a single writer can modify the state of
	 * the store, without affecting any readers. The subclass should, at a
	 * minimum, use this lock in the {@link #insert(Write)} method.
	 */
	protected final ReentrantLock writeLock = new ReentrantLock();

	/**
	 * A Predicate that is used to filter out empty sets.
	 */
	private static final Predicate<Set<? extends Object>> emptySetFilter = new Predicate<Set<? extends Object>>() {

		@Override
		public boolean apply(@Nullable Set<? extends Object> input) {
			return !input.isEmpty();
		}

	};

	/**
	 * Insert {@code write} into the store without performing any validity
	 * checks because this method is called from
	 * {@link #add(String, TObject, long)} and
	 * {@link #remove(String, TObject, long)}, which verify that {@code write}
	 * is valid for insertion. This subclass should implement any necessary
	 * write locking in this method.
	 * 
	 * @param write
	 * @return {@code true}
	 */
	protected abstract boolean insert(Write write);

	@Override
	public final boolean add(String key, TObject value, long record) {
		Write write = Write.add(key, value, record);
		return !verify(write) ? insert(write) : false;
	}

	@Override
	public Map<Long, String> audit(long record) {
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

	@Override
	public Map<Long, String> audit(String key, long record) {
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

	@Override
	public Set<String> describe(long record) {
		return describeUsingContext(record,
				Maps.<String, Set<TObject>> newHashMap());
	}

	@Override
	public Set<String> describe(long record, long timestamp) {
		return describeUsingContext(record, timestamp,
				Maps.<String, Set<TObject>> newHashMap());
	}

	@Override
	public Set<TObject> fetch(String key, long record) {
		return fetch(key, record, Time.now());
	}

	@Override
	public Set<TObject> fetch(String key, long record, long timestamp) {
		Set<TObject> values = Sets.newLinkedHashSet();
		Iterator<Write> it = iterator();
		while (it.hasNext()) {
			Write write = it.next();
			if(write.getVersion() <= timestamp) {
				if(key.equals(write.getKey().toString())
						&& Longs.compare(record, write.getRecord().longValue()) == 0) {
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

	@Override
	public Set<Long> find(long timestamp, String key, Operator operator,
			TObject... values) {
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
	public final boolean remove(String key, TObject value, long record) {
		Write write = Write.remove(key, value, record);
		return verify(write) ? insert(write) : false;
	}

	@Override
	public Set<Long> search(String key, String query) {
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

	/**
	 * Transport the content of this store to {@code destination}.
	 * 
	 * @param destination
	 */
	public void transport(PermanentStore destination) {
		Iterator<Write> it = iterator();
		while (it.hasNext()) {
			destination.accept(it.next());
			it.remove();
		}

	}

	@Override
	public boolean verify(String key, TObject value, long record) {
		return verify(key, value, record, Time.now());
	}

	@Override
	public boolean verify(String key, TObject value, long record, long timestamp) {
		return verify(Write.notStorable(key, value, record), timestamp);
	}

	/**
	 * Calculate the description for {@code record} at {@code timestamp} using
	 * information in {@code ktv} as if it were also a part of the Buffer. This
	 * method is used to accurately describe records using prior context about
	 * data that was transported from the Buffer to a destination.
	 * 
	 * @param record
	 * @param timestamp
	 * @param ktv
	 * @return a possibly empty Set of keys
	 */
	protected Set<String> describeUsingContext(long record, long timestamp,
			Map<String, Set<TObject>> ktv) {
		Iterator<Write> it = iterator();
		search: while (it.hasNext()) {
			Write write = it.next();
			if(write.getRecord().longValue() == record) {
				if(write.getVersion() <= timestamp) {
					Set<TObject> values;
					values = ktv.get(write.getKey().toString());
					if(values == null) {
						values = Sets.newHashSet();
						ktv.put(write.getKey().toString(), values);
					}
					if(write.getType() == Action.ADD) {
						values.add(write.getValue().getTObject());
					}
					else {
						values.remove(write.getValue().getTObject());
					}
				}
				else {
					break search;
				}
			}
		}
		return Maps.filterValues(ktv, emptySetFilter).keySet();
	}

	/**
	 * Calculate the description for {@code record} using information in
	 * {@code ktv} as if it were also a part of the Buffer. This method is used
	 * to accurately describe records using prior context about data that was
	 * transported from the Buffer to a destination.
	 * 
	 * @param record
	 * @param ktv
	 * @return a possibly empty Set of keys
	 */
	protected Set<String> describeUsingContext(long record,
			Map<String, Set<TObject>> ktv) {
		return describeUsingContext(record, Time.now(), ktv);
	}

	/**
	 * Return {@code true} if {@code write} represents a data mapping that
	 * currently exists.
	 * 
	 * @param write
	 * @return {@code true} if {@code write} currently appears an odd number of
	 *         times
	 */
	protected boolean verify(Write write) {
		return verify(write, Time.now());
	}

	/**
	 * Return {@code true} if {@code write} represents a data mapping that
	 * exists at {@code timestamp}.
	 * 
	 * @param write
	 * @param timestamp
	 * @return {@code true} if {@code write} appears an odd number of times at
	 *         {@code timestamp}
	 */
	protected boolean verify(Write write, long timestamp) {
		boolean exists = false;
		Iterator<Write> it = iterator();
		while (it.hasNext()) {
			Write stored = it.next();
			if(stored.getVersion() <= timestamp) {
				if(stored.equals(write)) {
					exists ^= true; // toggle boolean
				}
			}
			else {
				break;
			}
		}
		return exists;
	}

}
