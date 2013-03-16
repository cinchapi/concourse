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
package com.cinchapi.concourse.store;

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
import com.cinchapi.concourse.services.ConcourseService;
import com.cinchapi.concourse.structure.Commit;
import com.cinchapi.concourse.structure.Key;
import com.cinchapi.concourse.structure.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * <p>
 * A {@link ConcourseService} that is maintained entirely in memory.
 * </p>
 * <p>
 * Data in a VolatileDatabase takes up 3X more space than it would on disk. This
 * structure serves as a suitable cache or fast, albeit temporary, storage for
 * data that will eventually be persisted to disk.
 * </p>
 * 
 * @author jnelson
 */
public class VolatileDatabase extends ConcourseService {

	/**
	 * Return a new {@link VolatileDatabase} with enough capacity for the
	 * {@code expectedSize}.
	 * 
	 * @param expectedCapacity
	 *            - the expected number of commits
	 * @return the memory representation
	 */
	public static VolatileDatabase newInstancewithExpectedCapacity(
			int expectedCapacity) {
		return new VolatileDatabase(expectedCapacity);
	}

	private static final TreeMap<Value, Set<Key>> EMPTY_VALUE_INDEX = new TreeMap<Value, Set<Key>>(); // read-only
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
	 * maps a Value to a KeySet indicating the rows that contain the value. Use
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
	protected VolatileDatabase(int expectedCapacity) {
		this.ordered = Lists.newArrayListWithCapacity(expectedCapacity);
		this.counts = Maps.newHashMapWithExpectedSize(expectedCapacity);
		this.columns = Maps.newHashMapWithExpectedSize(expectedCapacity);
	}

	/**
	 * Return the number of commits that exist for the revision for
	 * {@code value} in the {@code cell} at the intersection of {@code row} and
	 * {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return the number of commits for the revision
	 */
	public int count(long row, String column, Object value) {
		return count(Commit.notForStorage(row, column, value));
	}

	/**
	 * Return a list of the commits in order.
	 * 
	 * @return the commits
	 */
	public List<Commit> getCommits() {
		synchronized (ordered) {
			return ordered;
		}
	}

	@Override
	protected boolean addSpi(long row, String column, Object value) {
		if(!exists(row, column, value)) {
			return record(Commit.forStorage(row, column, value));
		}
		return false;
	}

	/**
	 * Return the count for {@code commit} in the commitlog.
	 * 
	 * @param commit
	 * @return the count
	 */
	protected int count(Commit commit) {
		return counts.containsKey(commit) ? counts.get(commit) : 0;
	}

	@Override
	protected Set<String> describeSpi(long row) {
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

	/**
	 * Return {@code true} if {@code commit} has been committed an odd number of
	 * time.
	 * 
	 * @param commit
	 * @return {@code true} if {@code commit} exists
	 */
	protected boolean exists(Commit commit) {
		return Numbers.isOdd(count(commit));
	}

	@Override
	protected boolean existsSpi(long row, String column, Object value) {
		return exists(Commit.notForStorage(row, column, value));
	}

	@Override
	protected Set<Object> fetchSpi(long row, String column) {
		Set<Value> _values = Sets.newLinkedHashSet();
		ListIterator<Commit> commiterator = ordered
				.listIterator(ordered.size());
		while (commiterator.hasPrevious()) {
			Commit commit = commiterator.previous();
			if(Longs.compare(commit.getRow().asLong(), row) == 0
					&& commit.getColumn().equals(column)) {
				if(_values.contains(commit.getValue())) { // this means I've
															// encountered an
															// even number
															// commit for
															// row/column/value
															// which resulted
															// from a removal
					_values.remove(commit.getValue());
				}
				else {
					_values.add(commit.getValue());
				}
			}
		}
		Set<Object> values = Sets.newLinkedHashSetWithExpectedSize(_values
				.size());
		for (Value value : _values) {
			values.add(value.getQuantity());
		}
		return values;
	}

	@Override
	protected Set<Long> querySpi(String column, Operator operator,
			Object... values) {
		// Throughout this method I have to check if the value indexed in
		// #columns still exists because I do not deindex columns for
		// remove commits
		Set<Long> rows = Sets.newHashSet();
		Value val = Value.notForStorage(values[0]);

		if(operator == Operator.EQUALS) {
			Set<Key> keys = getKeySetForColumnAndValue(column, val);
			if(keys != null) {
				Object obj = val.getQuantity();
				for (Key key : keys) {
					long row = key.asLong();
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
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
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
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
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
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
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
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
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
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
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
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
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
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
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
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
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
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

	/**
	 * Record the {@code commit} in memory. This method DOES NOT perform any
	 * validation or input checks.
	 * 
	 * @param commit
	 * @return {@code true}
	 */
	protected boolean record(Commit commit) {
		int count = count(commit) + 1;
		counts.put(commit, count);
		ordered.add(commit);
		index(commit); // I won't deindex commits because it is
						// expensive and I will check the commit
						// #count() whenever I read from the index.
		return true;
	}

	@Override
	protected boolean removeSpi(long row, String column, Object value) {
		if(exists(row, column, value)) {
			return record(Commit.forStorage(row, column, value));
		}
		return false;
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
			rows = Sets.newHashSet();
			values.put(value, rows);
		}
		rows.add(row);
	}

}