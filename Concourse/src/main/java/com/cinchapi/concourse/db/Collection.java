/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.db;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.lock.Lock;
import com.cinchapi.common.lock.Lockable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * <p>
 * A Record is a {@link Sequence} that represents a logical grouping of related
 * data in the form of field names mapped to values. Records are optimized for
 * high levels of concurrency by offering external callers field level
 * read/write locking.
 * </p>
 * <p>
 * Records are similar to traditional database rows in their ability to
 * efficiently handle non-query reads, but are largely different because they
 * lack a predefined or regular schema and allow multiple distinct values to
 * exist for a single field.
 * </p>
 * 
 * @author jnelson
 */
final class Collection extends Sequence<SuperString, Value> {

	/**
	 * Return the row that is located by {@code key}.
	 * 
	 * @param key
	 * @return the row
	 */
	public static Collection fromKey(PrimaryKey key) {
		Collection row = cache.get(key.asLong());
		if(row == null) {
			row = new Collection(key);
			cache.put(row, key.asLong());
		}
		return row;
	}

	private static final Element mock = Container.mock(Element.class);
	private static final ObjectReuseCache<Collection> cache = new ObjectReuseCache<Collection>();
	protected static final String FILE_NAME_EXT = "ccr"; // @Override from
															// {@link Tuple}
	protected static final String LOCALE_HOME = "1";// @Override from
													// {@link Tuple}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	private Collection(PrimaryKey key) {
		super(key);
	}

	@Override
	protected Container<SuperString, Value> getBucketFromByteSequence(
			ByteBuffer bytes) {
		return new Element(bytes);
	}

	@Override
	protected Container<SuperString, Value> getMockBucket() {
		return mock;
	}

	@Override
	protected Container<SuperString, Value> getNewBucket(SuperString field) {
		return new Element(field);

	}

	@Override
	protected Map<SuperString, Container<SuperString, Value>> getNewBuckets(
			int expectedSize) {
		return Maps.newHashMapWithExpectedSize(expectedSize);
	}

	@Override
	void add(SuperString field, Value value) {
		super.add(field, value);
	}

	/**
	 * Return the fields that map to non-empty cells in the row.
	 * 
	 * @return the set of fields in the row
	 */
	Set<SuperString> describe() {
		return describe(false, 0);
	}

	/**
	 * Return the fields that map to non-empty cells in the row at the
	 * specified {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the set of fields in the row
	 */
	Set<SuperString> describe(long timestamp) {
		return describe(true, timestamp);
	}

	/**
	 * Return {@code true} if {@code value} exists in the cell mapped from
	 * {@code field}.
	 * 
	 * @param field
	 * @param value
	 * @return {@code true} if {@code value} exists in the cell mapped from
	 *         {@code field}
	 */
	boolean exists(SuperString field, Value value) {
		return exists(field, value, false, 0);
	}

	/**
	 * Return {@code true} if {@code value} exists in the cell mapped from
	 * {@code field} at the specified {@code timestamp}.
	 * 
	 * @param field
	 * @param value
	 * @param timestamp
	 * @return {@code true} if {@code value} exists in the cell mapped from
	 *         {@code field}
	 */
	boolean exists(SuperString field, Value value, long timestamp) {
		return exists(field, value, true, timestamp);
	}

	/**
	 * Return the set of values contained in the cell mapped from {@code field}
	 * 
	 * @param field
	 * @return the set of values
	 */
	Set<Value> fetch(SuperString field) {
		return fetch(field, false, 0);
	}

	/**
	 * Return the set of values contained in the cell mapped from {@code field}
	 * at the specified {@code timestamp}.
	 * 
	 * @param field
	 * @param timestamp
	 * @return the set of values
	 */
	Set<Value> fetch(SuperString field, long timestamp) {
		return fetch(field, true, timestamp);
	}

	/**
	 * Read lock the cell mapped from {@code field}
	 * 
	 * @param field
	 * @return the releasable lock
	 */
	Lock readLock(SuperString field) {
		return ((Element) getBucket(field)).readLock();
	}

	@Override
	void remove(SuperString field, Value value) throws IllegalArgumentException {
		super.remove(field, value);
	}

	/**
	 * Write lock the cell mapped from {@code field}.
	 * 
	 * @param field
	 * @return the releasable lock
	 */
	Lock writeLock(SuperString field) {
		return ((Element) getBucket(field)).writeLock();
	}

	/**
	 * Return the fields that map to non-empty cells in the row at the present
	 * or at the specified {@code timestamp} if {@code historical} is
	 * {@code true}.
	 * 
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @return the set of fields in the row
	 */
	private Set<SuperString> describe(boolean historical, long timestamp) {
		Lock lock = readLock();
		try {
			Set<SuperString> fields = Sets.newHashSetWithExpectedSize(buckets()
					.size());
			Iterator<Container<SuperString, Value>> it = buckets().values()
					.iterator();
			while (it.hasNext()) {
				Element cell = (Element) it.next();
				boolean empty = historical ? cell.getValues(timestamp)
						.isEmpty() : cell.isEmpty();
				if(!empty) {
					fields.add(cell.getKey());
				}
			}
			return fields;
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return {@code true} if {@code value} exists in the cell mapped from
	 * {@code field} at the present or at the specified {@code timestamp} if
	 * {@code historical} is set to {@code true}.
	 * 
	 * @param field
	 * @param value
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @return {@code true} if {@code value} exists in the cell mapped from
	 *         {@code field}
	 */
	private boolean exists(SuperString field, Value value, boolean historical,
			long timestamp) {
		Lock lock = readLock();
		try {
			return historical ? getBucket(field).getValues(timestamp).contains(
					value) : getBucket(field).getValues().contains(value);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return the set of values contained in the cell mapped from {@code field}
	 * at the present or at the specified {@code timestamp} if
	 * {@code historical} is set to {@code true}.
	 * 
	 * @param field
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @return the set of values
	 */
	private Set<Value> fetch(SuperString field, boolean historical,
			long timestamp) {
		Lock lock = readLock();
		try {
			Set<Value> values = Sets.newLinkedHashSet();
			Iterator<Value> it = historical ? getBucket(field).getValues(
					timestamp).iterator() : getBucket(field).getValues()
					.iterator();
			while (it.hasNext()) {
				values.add(it.next());
			}
			return values;
		}
		finally {
			lock.release();
		}
	}

	/**
	 * A single element within a {@link Collection}.
	 * 
	 * @author jnelson
	 */
	final static class Element extends LockableBucket<SuperString, Value> implements
			Lockable {

		/**
		 * Construct a new instance. Use this constructor when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 */
		Element(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param field
		 */
		Element(SuperString field) {
			super(field);
		}

		@Override
		protected SuperString getKeyFromByteSequence(ByteBuffer bytes) {
			return SuperString.fromBytes(bytes.array());
		}

		@Override
		protected Value getValueFromByteSequence(ByteBuffer bytes) {
			return Value.fromByteSequence(bytes);
		}
	}
}
