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
package com.cinchapi.concourse.internal;

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
 * The service stores data as a list of {@link Commit} objects with hyper
 * indexing<sup>1</sup>. The data takes up 3X more space than it would on disk.
 * This structure serves as a suitable cache or as fast, albeit temporary,
 * storage for data that will eventually be persisted to disk.
 * </p>
 * <p>
 * The service can theoretically handle up to {@value #MAX_NUM_COMMITS} writes,
 * but memory availability is a more reliable gauge.
 * </p>
 * <p>
 * <sup>1</sup> - A red-black tree is used to index <em>every</em> column for
 * logarithmic query operations. Additionally, the count for each distinct
 * commit is maintained, so the ability to determine if a value exists in a cell
 * takes constant time. Finally, no special row-oriented indices are maintained
 * so fetch and describe operations run in linear time.
 * </p>
 * 
 * @author jnelson
 */
class VolatileStorage extends ConcourseService implements IndexingService {

	/**
	 * Return a new {@link VolatileStorage} with enough capacity for the
	 * {@code expectedSize}.
	 * 
	 * @param expectedCapacity
	 *            - the expected number of commits
	 * @return the memory representation
	 */
	static VolatileStorage newInstancewithExpectedCapacity(
			int expectedCapacity) {
		return new VolatileStorage(expectedCapacity);
	}

	/**
	 * The maximum number of commits that can be stored by the service.
	 */
	public static final int MAX_NUM_COMMITS = Integer.MAX_VALUE;

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
	 * Maintains all the commits in chronological order. Elements from
	 * the list SHOULD NOT be deleted, so handle with care.
	 */
	protected List<Commit> ordered;

	/**
	 * Maintains a mapping from a Commit to the number of times the Commit
	 * exists in {@link #ordered}.
	 */
	protected HashMap<Commit, Integer> counts;

	/**
	 * Maintains an index mapping a column name to a ValueIndex. The ValueIndex
	 * maps a Value to a KeySet holding the rows that contain the value. Use
	 * helper functions to retrieve data from this index so as to avoid
	 * NullPointerExceptions.
	 * 
	 * @see {@link #getValueIndexForColumn(String)}
	 * @see {@link #getKeySetForColumnAndValue(String, Value)}
	 */
	private HashMap<String, TreeMap<Value, Set<Key>>> columns; // treemap is a
																// red-black
																// tree

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
		for (Commit commit : ordered) {
			if(contains(commit)) {
				index(commit);
			}
		}
	}

	@Override
	public synchronized void shutdown() {/* do nothing */}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		return commit(Commit.forStorage(column, value, row), true);
	}

	/**
	 * Record the {@code commit} in memory. This method DOES NOT perform any
	 * validation or input checks.
	 * 
	 * @param commit
	 * @param index
	 *            - set to {@code true} to index the commit for
	 *            {@link #query(String, Operator, Object...)} operations
	 * @return {@code true}
	 */
	protected final boolean commit(final Commit commit, boolean index) {
		// I wont deindex from a remove commit because it is expensive and I
		// will check the commit #count() whenever I read from the index. To
		// reclaim space for stale indices, call reindex()
		int count = count(commit) + 1;
		counts.put(commit, count);
		ordered.add(commit);
		if(index) {
			index(commit);
		}
		return true;
	}

	/**
	 * Return {@code true} if {@code commit} has been committed an odd number of
	 * times and is therefore considered to be contained (meaning the committed
	 * value exists).
	 * 
	 * @param commit
	 * @return {@code true} if {@code commit} exists.
	 */
	protected final boolean contains(Commit commit) {
		return Numbers.isOdd(count(commit));
	}

	/**
	 * Return the count for {@code commit} in the storage. Many operations
	 * build upon this functionality (i.e the {@code exists} method, which is
	 * called by both the {@code add} and {@code remove} methods
	 * before issuing writes is built upon this method.
	 * 
	 * @param commit
	 * @return the count
	 */
	protected final int count(Commit commit) {
		commit = Commit.notForStorageCopy(commit);
		synchronized (commit) { // I can lock locally here because a
								// notForStorage commit is a cached reference
			return counts.containsKey(commit) ? counts.get(commit) : 0;
		}
	}

	@Override
	protected final Set<String> describeSpi(long row) { // O(n)
		Map<String, Set<Value>> columns2Values = Maps.newHashMap();
		synchronized (ordered) {
			Iterator<Commit> commiterator = ordered.iterator();
			while (commiterator.hasNext()) {
				Commit commit = commiterator.next();
				if(Longs.compare(commit.getRow().asLong(), row) == 0) {
					Set<Value> values;
					if(columns2Values.containsKey(commit.getColumn())) {
						values = columns2Values.get(commit.getColumn());
					}
					else {
						values = Sets.newHashSet();
						columns2Values.put(commit.getColumn(), values);
					}
					if(values.contains(commit.getValue())) { // this means I've
																// encountered
																// an
																// even number
																// commit for
																// row/column/value
																// which
																// resulted
																// from a
																// removal
						values.remove(commit.getValue());
					}
					else {
						values.add(commit.getValue());
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
		return contains(Commit.notForStorage(column, value, row));
	}

	@Override
	protected final Set<Object> fetchSpi(String column, long timestamp, long row) { // O(n)
		Set<Value> _values = Sets.newLinkedHashSet();
		ListIterator<Commit> commiterator = ordered.listIterator();
		while (commiterator.hasNext()) {
			Commit commit = commiterator.next();
			if(commit.getValue().getTimestamp() <= timestamp) {
				if(Longs.compare(commit.getRow().asLong(), row) == 0
						&& commit.getColumn().equals(column)) {
					if(_values.contains(commit.getValue())) { // this means I've
																// encountered
																// an
																// even number
																// commit for
																// row/column/value
																// which
																// resulted
																// from a
																// removal
						_values.remove(commit.getValue());
					}
					else {
						_values.add(commit.getValue());
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
	 * still exists because I do not deindex columns for remove commits
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
					Commit commit = Commit.notForStorage(column, obj, row);
					if(contains(commit)) {
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
						Commit commit = Commit.notForStorage(column, obj, row);
						if(contains(commit)) {
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
					Commit commit = Commit.notForStorage(column, obj, row);
					if(contains(commit)) {
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
					Commit commit = Commit.notForStorage(column, obj, row);
					if(contains(commit)) {
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
					Commit commit = Commit.notForStorage(column, obj, row);
					if(contains(commit)) {
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
					Commit commit = Commit.notForStorage(column, obj, row);
					if(contains(commit)) {
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
					Commit commit = Commit.notForStorage(column, obj, row);
					if(contains(commit)) {
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
						Commit commit = Commit.notForStorage(column, obj, row);
						if(contains(commit)) {
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
				Matcher m = p.matcher(theVal.toString());
				Set<Key> keys = entry.getValue();
				if(!m.matches()) {
					for (Key key : keys) {
						long row = key.asLong();
						Commit commit = Commit.notForStorage(column, obj, row);
						if(contains(commit)) {
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
		return commit(Commit.forStorage(column, value, row), false);
	}

	@Override
	protected long sizeOfSpi(String column, Long row) { // O(n)
		long size = 0;
		boolean seekingSizeForDb = row == null && column == null;
		boolean seekingSizeForRow = row != null && column == null;
		boolean seekingSizeForCell = row != null && column != null;
		synchronized (ordered) {
			Iterator<Commit> commiterator = ordered.iterator();
			while (commiterator.hasNext()) {
				Commit commit = commiterator.next();
				boolean inRow = seekingSizeForRow
						&& Longs.compare(commit.getRow().asLong(), row) == 0; // prevents
																				// NPE
				boolean inCell = seekingSizeForCell
						&& Longs.compare(commit.getRow().asLong(), row) == 0
						&& commit.getColumn().equals(column); // prevents NPE
				if(seekingSizeForDb || inRow || inCell) {
					size += commit.size();
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
	 * Add indexes for the commit to allow for more efficient
	 * {@link #query(String, com.cinchapi.concourse.store.api.Queryable.Operator, Object...)}
	 * operations.
	 * 
	 * @param commit
	 */
	private void index(Commit commit) {
		String column = commit.getColumn();
		Value value = commit.getValue();
		Key row = commit.getRow();
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