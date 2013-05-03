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
package com.cinchapi.concourse.engine.old;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.db.services.IndexService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * <p>
 * A lightweight {@link ConcourseService} that is maintained entirely in memory.
 * </p>
 * <p>
 * The service stores data as a list of {@link Write} objects with partial
 * indexing<sup>1</sup>. The data takes up 3X more space than it would on disk.
 * This structure serves as a suitable cache or as fast, albeit temporary,
 * storage for data that will eventually be persisted to disk.
 * </p>
 * <p>
 * The service can theoretically handle up to {@value #MAX_NUM_WRITES} writes,
 * but memory availability is a more reliable gauge.
 * </p>
 * <p>
 * <sup>1</sup> - A red-black tree is used to index <em>every</em> column for
 * logarithmic query operations. Additionally, the count for each distinct write
 * is maintained, so the ability to determine if a value exists in a cell takes
 * constant time. Finally, no special row-oriented indices are maintained so
 * fetch and describe operations run in linear time.
 * </p>
 * 
 * @author jnelson
 */
class VolatileStorage extends ConcourseService implements IndexService {

	/**
	 * Return a new {@link VolatileStorage} with enough capacity for the
	 * {@code expectedSize}.
	 * 
	 * @param expectedCapacity
	 *            - the expected number of writes
	 * @return the memory representation
	 */
	static VolatileStorage newInstancewithExpectedCapacity(int expectedCapacity) {
		return new VolatileStorage(expectedCapacity);
	}

	/**
	 * The maximum number of writes that can be stored by the service.
	 */
	public static final int MAX_NUM_WRITES = Integer.MAX_VALUE;

	/**
	 * The average number of rows that have value V in column C. This number is
	 * used to set the initial capacity of each KeySet. Setting this value too
	 * high will consume more memory, but setting it too low will slow down
	 * writes because the KeySet will need to be resized.
	 */
	private static final int AVG_NUM_ROWS_MAPPED_FROM_SINGLE_VALUE_IN_COLUMN = 100;
	private static final TreeMap<Value, Set<Key>> EMPTY_VALUE_INDEX = new TreeMap<Value, Set<Key>>(); // treat
																										// as
																										// read-only
	private static final Set<Key> EMPTY_KEY_SET = Collections
			.unmodifiableSet(new HashSet<Key>());
	private static final Comparator<Value> comparator = new Value.LogicalComparator();

	/**
	 * Maintains all the writes in chronological order. Elements from
	 * the list SHOULD NOT be deleted, so handle with care.
	 */
	protected List<Write> ordered;

	/**
	 * Maintains a mapping from a Write to the number of times the Write
	 * exists in {@link #ordered}.
	 */
	protected HashMap<Write, Integer> counts;

	/**
	 * Maintains an index mapping a column name to a ValueIndex. The ValueIndex
	 * maps a Value to a KeySet holding the rows that contain the value. Use
	 * helper functions to retrieve data from this index so as to avoid
	 * NullPointerExceptions.
	 * 
	 * @see {@link #getValueIndexForColumn(String)}
	 * @see {@link #getKeySetForColumnAndValue(String, Value)}
	 */
	private HashMap<String, TreeMap<Value, Set<Key>>> columns; 

	/**
	 * Construct a new empty instance with the {@code expectedCapacity}.
	 * 
	 * @param expectedCapacity
	 */
	protected VolatileStorage(int expectedCapacity) {
		this.ordered = Lists.newArrayListWithCapacity(expectedCapacity);
		this.counts = Maps.newHashMapWithExpectedSize(expectedCapacity);
		this.columns = Maps.newHashMapWithExpectedSize(expectedCapacity);
	}

	@Override
	public synchronized void reindex() { // O(n)
		for (Write write : ordered) {
			if(contains(write)) {
				index(write);
			}
		}
	}

	@Override
	public synchronized void shutdown() {/* do nothing */}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		return commit(Write.forStorage(column, value, row, WriteType.ADD), true);
	}

	/**
	 * Record the {@code write} in memory. This method DOES NOT perform any
	 * validation or input checks.
	 * 
	 * @param write
	 * @param index
	 *            - set to {@code true} to index the write for
	 *            {@link #query(String, Operator, Object...)} operations
	 * @return {@code true}
	 */
	protected final boolean commit(final Write write, boolean index) {
		// I wont deindex from a remove write because it is expensive and I
		// will check the write #count() whenever I read from the index. To
		// reclaim space for stale indices, call reindex()
		int count = count(write) + 1;
		counts.put(write, count);
		ordered.add(write);
		if(index) {
			index(write);
		}
		return true;
	}

	/**
	 * Return {@code true} if {@code write} has been committed an odd number of
	 * times and is therefore considered to be contained (meaning the committed
	 * value exists).
	 * 
	 * @param write
	 * @return {@code true} if {@code write} exists.
	 */
	protected final boolean contains(Write write) {
		return Numbers.isOdd(count(write));
	}

	/**
	 * Return the count for {@code write} in the storage. Many operations
	 * build upon this functionality (i.e the {@code exists} method, which is
	 * called by both the {@code add} and {@code remove} methods
	 * before issuing writes.
	 * 
	 * @param write
	 * @return the count
	 */
	protected final int count(Write write) {
		write = Write.notForStorageCopy(write);
		synchronized (write) { // I can lock locally here because a
								// notForStorage write is a cached reference
			return counts.containsKey(write) ? counts.get(write) : 0;
		}
	}

	@Override
	protected final Set<String> describeSpi(long row) { // O(n)
		Map<String, Set<Value>> columns2Values = Maps.newHashMap();
		synchronized (ordered) {
			Iterator<Write> writeIterator = ordered.iterator();
			while (writeIterator.hasNext()) {
				Write write = writeIterator.next();
				if(Longs.compare(write.getRow().asLong(), row) == 0) {
					Set<Value> values;
					if(columns2Values.containsKey(write.getColumn())) {
						values = columns2Values.get(write.getColumn());
					}
					else {
						values = Sets.newHashSet();
						columns2Values.put(write.getColumn(), values);
					}
					if(values.contains(write.getValue())) { // this means I've
															// encountered
															// an
															// even number
															// write for
															// row/column/value
															// which
															// resulted
															// from a
															// removal
						values.remove(write.getValue());
					}
					else {
						values.add(write.getValue());
					}
				}
			}
			Set<String> columns = columns2Values.keySet();
			Iterator<String> coliterator = columns.iterator();
			while (coliterator.hasNext()) {
				if(columns2Values.get(coliterator.next()).isEmpty()) {
					coliterator.remove();
				}
			}
			return columns;
		}
	}

	@Override
	protected final boolean existsSpi(String column, Object value, long row) {
		return contains(Write.notForStorage(column, value, row));
	}

	@Override
	protected final Set<Object> fetchSpi(String column, long timestamp, long row) { // O(n)
		Set<Value> _values = Sets.newLinkedHashSet();
		ListIterator<Write> writeIterator = ordered.listIterator();
		while (writeIterator.hasNext()) {
			Write write = writeIterator.next();
			if(write.getValue().getTimestamp() <= timestamp) {
				if(Longs.compare(write.getRow().asLong(), row) == 0
						&& write.getColumn().equals(column)) {
					if(_values.contains(write.getValue())) { // this means I've
																// encountered
																// an
																// even number
																// write for
																// row/column/value
																// which
																// resulted
																// from a
																// removal
						_values.remove(write.getValue());
					}
					else {
						_values.add(write.getValue());
					}
				}
			}
			else {
				break;
			}
		}
		Set<Object> values = Sets.newLinkedHashSetWithExpectedSize(_values
				.size());
		for (Value value : _values) {
			values.add(value.getQuantity());
		}
		return values;
	}

	/*
	 * (non-Javadoc)
	 * Throughout this method I have to check if the value indexed in #columns
	 * still exists because I do not deindex columns for removes
	 */
	@Override
	protected final Set<Long> querySpi(String column, Operator operator,
			Object... values) {
		Set<Long> rows = Sets.newLinkedHashSet();
		Value val = Value.notForStorage(values[0]);

		if(operator == Operator.EQUALS) {
			Set<Key> keys = getKeySetForColumnAndValue(column, val);
			if(keys != null) {
				Object obj = val.getQuantity();
				for (Key key : keys) {
					long row = key.asLong();
					Write write = Write.notForStorage(column, obj, row);
					if(contains(write)) {
						rows.add(row);
					}
				}
			}
		}
		else if(operator == Operator.NOT_EQUALS) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				if(!theVal.equals(val)) {
					Set<Key> keys = entry.getValue();
					for (Key key : keys) {
						long row = key.asLong();
						Write write = Write.notForStorage(column, obj, row);
						if(contains(write)) {
							rows.add(key.asLong());
						}
					}
				}
			}
		}
		else if(operator == Operator.GREATER_THAN) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.tailMap(val, false).entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Set<Key> keys = entry.getValue();
				for (Key key : keys) {
					long row = key.asLong();
					Write write = Write.notForStorage(column, obj, row);
					if(contains(write)) {
						rows.add(key.asLong());
					}
				}
			}
		}
		else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.tailMap(val, true).entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Set<Key> keys = entry.getValue();
				for (Key key : keys) {
					long row = key.asLong();
					Write write = Write.notForStorage(column, obj, row);
					if(contains(write)) {
						rows.add(key.asLong());
					}
				}
			}
		}
		else if(operator == Operator.LESS_THAN) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.headMap(val, false).entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Set<Key> keys = entry.getValue();
				for (Key key : keys) {
					long row = key.asLong();
					Write write = Write.notForStorage(column, obj, row);
					if(contains(write)) {
						rows.add(key.asLong());
					}
				}
			}
		}
		else if(operator == Operator.LESS_THAN_OR_EQUALS) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.headMap(val, true).entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Set<Key> keys = entry.getValue();
				for (Key key : keys) {
					long row = key.asLong();
					Write write = Write.notForStorage(column, obj, row);
					if(contains(write)) {
						rows.add(key.asLong());
					}
				}
			}
		}
		else if(operator == Operator.BETWEEN) {
			Preconditions.checkArgument(values.length > 1,
					"You must specify two arguments for the BETWEEN selector.");
			Value v2 = Value.notForStorage(values[1]);
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.subMap(val, true, v2, false).entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Set<Key> keys = entry.getValue();
				for (Key key : keys) {
					long row = key.asLong();
					Write write = Write.notForStorage(column, obj, row);
					if(contains(write)) {
						rows.add(key.asLong());
					}
				}
			}
		}
		else if(operator == Operator.REGEX) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Pattern p = Pattern.compile(values[0].toString());
				Matcher m = p.matcher(theVal.toString());
				Set<Key> keys = entry.getValue();
				if(m.matches()) {
					for (Key key : keys) {
						long row = key.asLong();
						Write write = Write.notForStorage(column, obj, row);
						if(contains(write)) {
							rows.add(key.asLong());
						}
					}
				}
			}
		}
		else if(operator == Operator.NOT_REGEX) {
			Iterator<Entry<Value, Set<Key>>> it = getValueIndexForColumn(column)
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, Set<Key>> entry = it.next();
				Value theVal = entry.getKey();
				Object obj = theVal.getQuantity();
				Pattern p = Pattern.compile(values[0].toString());
				Matcher m = p.matcher(obj.toString());
				Set<Key> keys = entry.getValue();
				if(!m.matches()) {
					for (Key key : keys) {
						long row = key.asLong();
						Write write = Write.notForStorage(column, obj, row);
						if(contains(write)) {
							rows.add(key.asLong());
						}
					}
				}
			}
		}
		else {
			throw new UnsupportedOperationException(operator
					+ " operator is unsupported");
		}
		return rows;
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		return commit(Write.forStorage(column, value, row, WriteType.REMOVE),
				false);
	}

	@Override
	protected long sizeOfSpi(String column, Long row) { // O(n)
		long size = 0;
		boolean seekingSizeForDb = row == null && column == null;
		boolean seekingSizeForRow = row != null && column == null;
		boolean seekingSizeForCell = row != null && column != null;
		synchronized (ordered) {
			Iterator<Write> writeIterator = ordered.iterator();
			while (writeIterator.hasNext()) {
				Write write = writeIterator.next();
				boolean inRow = seekingSizeForRow
						&& Longs.compare(write.getRow().asLong(), row) == 0; // prevents
																				// NPE
				boolean inCell = seekingSizeForCell
						&& Longs.compare(write.getRow().asLong(), row) == 0
						&& write.getColumn().equals(column); // prevents NPE
				if(seekingSizeForDb || inRow || inCell) {
					size += write.size();
				}
			}
			return size;
		}
	}

	/**
	 * Safely return a KeySet for the rows that contain {@code value} in
	 * {@code column}.
	 * 
	 * @param column
	 * @param value
	 * @return the KeySet
	 */
	private Set<Key> getKeySetForColumnAndValue(String column, Value value) {
		TreeMap<Value, Set<Key>> valueIndex = getValueIndexForColumn(column);
		if(valueIndex.containsKey(value)) {
			return valueIndex.get(value);
		}
		return EMPTY_KEY_SET;
	}

	/**
	 * Safely return a ValueIndex for {@code column}.
	 * 
	 * @param column
	 * @return the ValueIndex
	 */
	private TreeMap<Value, Set<Key>> getValueIndexForColumn(String column) {
		if(columns.containsKey(column)) {
			return columns.get(column);
		}
		return EMPTY_VALUE_INDEX;
	}

	/**
	 * Add indexes for the write to allow for more efficient
	 * {@link #query(String, com.cinchapi.concourse.store.api.Queryable.Operator, Object...)}
	 * operations.
	 * 
	 * @param write
	 */
	private void index(Write write) {
		String column = write.getColumn();
		Value value = write.getValue();
		Key row = write.getRow();
		TreeMap<Value, Set<Key>> values;
		if(columns.containsKey(column)) {
			values = columns.get(column);
		}
		else {
			values = Maps.newTreeMap(comparator);
			columns.put(column, values);
		}
		Set<Key> rows;
		if(values.containsKey(value)) {
			rows = values.get(value);
		}
		else {
			rows = Sets
					.newHashSetWithExpectedSize(AVG_NUM_ROWS_MAPPED_FROM_SINGLE_VALUE_IN_COLUMN);
			values.put(value, rows);
		}
		rows.add(row);
	}

}