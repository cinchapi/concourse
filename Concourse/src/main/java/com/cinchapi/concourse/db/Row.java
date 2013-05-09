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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.lock.Lock;
import com.cinchapi.common.lock.Lockable;
import com.cinchapi.common.lock.Lockables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A Row is a collection of cells, each mapped from a column name and containing
 * a collection of {@link Value} objects.
 * 
 * @author jnelson
 */
final class Row extends BucketMap<ByteSizedString, Value> {

	/**
	 * Return the row that is located by {@code key}.
	 * 
	 * @param key
	 * @return the row
	 */
	public static Row fromKey(PrimaryKey key) {
		Row row = cache.get(key.asLong());
		if(row == null) {
			row = new Row(key);
			cache.put(row, key.asLong());
		}
		return row;
	}

	private static final Cell mock = Bucket.mock(Cell.class);
	private static final ObjectReuseCache<Row> cache = new ObjectReuseCache<Row>();
	protected static final String FILE_NAME_EXT = "ccr"; // @Override from
															// {@link Store}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	private Row(PrimaryKey key) {
		super(key);
	}

	@Override
	protected Bucket<ByteSizedString, Value> getBucketFromByteSequence(
			ByteBuffer bytes) {
		return new Cell(bytes);
	}

	@Override
	protected Bucket<ByteSizedString, Value> getMockBucket() {
		return mock;
	}

	@Override
	protected Bucket<ByteSizedString, Value> getNewBucket(ByteSizedString column) {
		return new Cell(column);

	}

	@Override
	protected Map<ByteSizedString, Bucket<ByteSizedString, Value>> getNewBuckets(
			int expectedSize) {
		return Maps.newHashMapWithExpectedSize(expectedSize);
	}

	/**
	 * Return the columns that map to non-empty cells in the row.
	 * 
	 * @return the set of columns in the row
	 */
	Set<ByteSizedString> describe() {
		return describe(false, 0);
	}

	/**
	 * Return the columns that map to non-empty cells in the row at the
	 * specified {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the set of columns in the row
	 */
	Set<ByteSizedString> describe(long timestamp) {
		return describe(true, timestamp);
	}

	/**
	 * Return {@code true} if {@code value} exists in the cell mapped from
	 * {@code column}.
	 * 
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} exists in the cell mapped from
	 *         {@code column}
	 */
	boolean exists(ByteSizedString column, Value value) {
		return exists(column, value, false, 0);
	}

	/**
	 * Return {@code true} if {@code value} exists in the cell mapped from
	 * {@code column} at the specified {@code timestamp}.
	 * 
	 * @param column
	 * @param value
	 * @param timestamp
	 * @return {@code true} if {@code value} exists in the cell mapped from
	 *         {@code column}
	 */
	boolean exists(ByteSizedString column, Value value, long timestamp) {
		return exists(column, value, true, timestamp);
	}

	/**
	 * Return the set of values contained in the cell mapped from {@code column}
	 * 
	 * @param column
	 * @return the set of values
	 */
	Set<Value> fetch(ByteSizedString column) {
		return fetch(column, false, 0);
	}

	/**
	 * Return the set of values contained in the cell mapped from {@code column}
	 * at the specified {@code timestamp}.
	 * 
	 * @param column
	 * @param timestamp
	 * @return the set of values
	 */
	Set<Value> fetch(ByteSizedString column, long timestamp) {
		return fetch(column, true, timestamp);
	}

	/**
	 * Read lock the cell mapped from {@code column}
	 * 
	 * @param column
	 * @return the releasable lock
	 */
	Lock readLock(ByteSizedString column) {
		return ((Cell) get(column)).readLock();
	}

	/**
	 * Write lock the cell mapped from {@code column}.
	 * 
	 * @param column
	 * @return the releasable lock
	 */
	Lock writeLock(ByteSizedString column) {
		return ((Cell) get(column)).writeLock();
	}

	/**
	 * Return the columns that map to non-empty cells in the row at the present
	 * or at the specified {@code timestamp} if {@code historical} is
	 * {@code true}.
	 * 
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @return the set of columns in the row
	 */
	private Set<ByteSizedString> describe(boolean historical, long timestamp) {
		Lock lock = readLock();
		try {
			Set<ByteSizedString> columns = Sets
					.newHashSetWithExpectedSize(buckets().size());
			Iterator<Bucket<ByteSizedString, Value>> it = buckets().values()
					.iterator();
			while (it.hasNext()) {
				Cell cell = (Cell) it.next();
				boolean empty = historical ? cell.getValues(timestamp)
						.isEmpty() : cell.isEmpty();
				if(!empty) {
					columns.add(cell.getKey());
				}
			}
			return columns;
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return {@code true} if {@code value} exists in the cell mapped from
	 * {@code column} at the present or at the specified {@code timestamp} if
	 * {@code historical} is set to {@code true}.
	 * 
	 * @param column
	 * @param value
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @return {@code true} if {@code value} exists in the cell mapped from
	 *         {@code column}
	 */
	private boolean exists(ByteSizedString column, Value value,
			boolean historical, long timestamp) {
		Lock lock = readLock();
		try {
			return historical ? get(column).getValues(timestamp)
					.contains(value) : get(column).getValues().contains(value);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Return the set of values contained in the cell mapped from {@code column}
	 * at the present or at the specified {@code timestamp} if
	 * {@code historical} is set to {@code true}.
	 * 
	 * @param column
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @return the set of values
	 */
	private Set<Value> fetch(ByteSizedString column, boolean historical,
			long timestamp) {
		Lock lock = readLock();
		try {
			Set<Value> values = Sets.newLinkedHashSet();
			Iterator<Value> it = historical ? get(column).getValues(timestamp)
					.iterator() : get(column).getValues().iterator();
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
	 * <p>
	 * The bucketed view of stored data from the perspective of a {@link Row}
	 * that is designed to efficiently handle non-query reads.
	 * </p>
	 * <p>
	 * This class models the traditional notion of a table cell because it is
	 * identified by the name of the column under which it sits and maintains a
	 * collection of values.
	 * </p>
	 * <p>
	 * Each Cell is individually {@link Lockable}, which enables maximum levels
	 * of concurrency.
	 * </p>
	 * 
	 * @author jnelson
	 */
	final static class Cell extends Bucket<ByteSizedString, Value> implements
			Lockable {
		// NOTE: This class is nested because the Row mostly abstracts away the
		// notion of a Bucket.

		private final transient ReentrantReadWriteLock locksmith = new ReentrantReadWriteLock();

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 */
		Cell(ByteSizedString column) {
			super(column);
		}

		/**
		 * Construct a new instance. Use this constructor when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 */
		Cell(ByteBuffer bytes) {
			super(bytes);
		}

		@Override
		public Lock readLock() {
			return Lockables.readLock(this, locksmith);
		}

		@Override
		public Lock writeLock() {
			return Lockables.writeLock(this, locksmith);
		}

		@Override
		protected ByteSizedString getKeyFromByteSequence(ByteBuffer bytes) {
			return ByteSizedString.fromBytes(bytes.array());
		}

		@Override
		protected Value getValueFromByteSequence(ByteBuffer bytes) {
			return Value.fromByteSequence(bytes);
		}
	}
}
