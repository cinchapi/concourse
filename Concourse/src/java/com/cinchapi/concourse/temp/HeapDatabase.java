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
package com.cinchapi.concourse.temp;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.api.Queryable.SelectOperator;
import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.db.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@link Commit} database that is maintained entirely in memory, enabling
 * faster read/write operations. The data takes up 3X more space in memory than
 * it would on disk.
 * 
 * @author Jeff Nelson
 */
public final class HeapDatabase {

	/**
	 * Return a new {@link HeapDatabase} with enough capacity for the
	 * {@code expectedSize}.
	 * 
	 * @param expectedCapacity
	 * @return the memory representation
	 */
	public static HeapDatabase newInstancewithExpectedSize(int expectedCapacity) {
		List<Commit> ordered = Lists.newArrayListWithCapacity(expectedCapacity);
		Map<Commit, Integer> counts = Maps
				.newHashMapWithExpectedSize(expectedCapacity);
		Map<String, TreeMap<Value, Set<Key>>> columns = Maps
				.newHashMapWithExpectedSize(expectedCapacity);
		return new HeapDatabase(ordered, counts, columns);
	}

	private static final Comparator<Value> comp = new Comparator<Value>() {

		@Override
		public int compare(Value o1, Value o2) {
			return o1.compareToLogically(o2);
		}

	};

	private List<Commit> ordered;
	private Map<Commit, Integer> counts;
	private Map<String, TreeMap<Value, Set<Key>>> columns;

	/**
	 * Construct a new instance.
	 * 
	 * @param ordered
	 * @param counts
	 */
	private HeapDatabase(List<Commit> ordered, Map<Commit, Integer> counts,
			Map<String, TreeMap<Value, Set<Key>>> columns) {
		this.ordered = ordered;
		this.counts = counts;
		this.columns = columns;
	}

	/**
	 * Add the {@code commit}.
	 * 
	 * @param commit
	 */
	public void add(Commit commit) {
		int count = count(commit) + 1;
		counts.put(commit, count);
		ordered.add(commit);
		index(commit); // I won't deindex commits in-memory because it is
						// expensive and I can will check the commit
						// #count() whenever I read from the index.
	}

	/**
	 * Return the count for {@code commit} in the commitlog.
	 * 
	 * @param commit
	 * @return the count
	 */
	public int count(Commit commit) {
		return counts.containsKey(commit) ? counts.get(commit) : 0;
	}

	/**
	 * Return {@code true} if {@code commit} has been committed an odd number of
	 * time.
	 * 
	 * @param commit
	 * @return {@code true} if {@code commit} exists
	 */
	public boolean exists(Commit commit) {
		return Numbers.isOdd(count(commit));
	}

	/**
	 * Return a list of the commits in order.
	 * 
	 * @return the commits
	 */
	public List<Commit> getCommits() {
		return ordered;
	}

	/**
	 * Implement the interface for
	 * {@link CommitLog#selectSpi(String, com.cinchapi.concourse.api.Queryable.SelectOperator, Object...)}
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the rows that satisfy the select criteria
	 */
	public Set<Long> select(String column, SelectOperator operator,
			Object... values) {
		// Throughout this method I have to check if the value indexed in
		// #columns still exists because I do not deindex columns for
		// removal commits
		Set<Long> rows = Sets.newHashSet();

		Value val = Value.notForStorage(values[0]);

		if(operator == SelectOperator.EQUALS) {
			Set<Key> keys = columns.get(column).get(val);
			Object obj = val.getQuantity();
			for (Key key : keys) {
				long row = key.asLong();
				Commit commit = Commit.notForStorage(row, column, obj);
				if(exists(commit)) {
					rows.add(row);
				}
			}
		}
		else if(operator == SelectOperator.NOT_EQUALS) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.GREATER_THAN) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.GREATER_THAN_OR_EQUALS) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.LESS_THAN) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.LESS_THAN_OR_EQUALS) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.BETWEEN) {
			Preconditions.checkArgument(values.length > 1,
					"You must specify two arguments for the BETWEEN selector.");
			Value v2 = Value.notForStorage(values[1]);
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.REGEX) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
		else if(operator == SelectOperator.NOT_REGEX) {
			Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
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
	 * Add indexes for the commit to allow for more efficient
	 * {@link #select(String, com.cinchapi.concourse.api.Queryable.SelectOperator, Object...)}
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
			values = Maps.newTreeMap(comp);
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