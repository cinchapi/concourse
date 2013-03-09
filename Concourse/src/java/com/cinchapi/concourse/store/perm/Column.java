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
package com.cinchapi.concourse.store.perm;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.store.api.Queryable.SelectOperator;
import com.cinchapi.concourse.store.search.Searcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A collection of {@link Value} where each is mapped to a {@link Key}.
 * 
 * @author jnelson
 */
public final class Column {

	/**
	 * Return an empty column.
	 * 
	 * @param name
	 * @return the column.
	 */
	public static Column createEmpty(String name) {
		return new Column(name);
	}

	private static final Logger log = LoggerFactory.getLogger(Column.class);
	private static final int maxNameSizeInBytes = 65536; // 64KB
	private static final Searcher searcher = null; //TODO get a searcher
	
	private final Select select = new Select();
	private final String name;
	private final ConcurrentSkipListMap<Value, RowSet> values = new ConcurrentSkipListMap<Value, RowSet>();

	/**
	 * Construct a new instance.
	 * 
	 * @param name
	 */
	protected Column(String name) {
		Preconditions.checkArgument(!name.contains(" "),
				"'%s' is an invalid column name because it contains spaces",
				name);
		Preconditions.checkArgument(Strings.isNullOrEmpty(name),
				"column name cannot be empty");
		Preconditions
				.checkArgument(
						name.getBytes(ByteBuffers.charset()).length < maxNameSizeInBytes,
						"column name cannot be larger than %s bytes",
						maxNameSizeInBytes);
		this.name = name;
	}

	/**
	 * Add {@code row} to {@code value}.
	 * 
	 * @param row
	 * @param value
	 * @return {@code true} if {@code row} is added.
	 */
	public boolean add(Key row, Value value) {
		Preconditions.checkNotNull(row);
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(value.isForStorage(),
				"'%s' is notForStorage and cannot be added", value);

		RowSet rows;
		if(values.containsKey(value)) {
			rows = values.get(value);
		}
		else {
			rows = RowSet.createEmpty();
			values.put(value, rows);
		}
		return rows.add(row);
	}

	/**
	 * Remove {@code row} from {@code value}.
	 * 
	 * @param row
	 * @param value
	 * @return {@code true} if {@code row} is removed.
	 */
	public boolean remove(Key row, Value value) {
		if(values.containsKey(value) && values.get(value).remove(row)) {
			if(values.get(value).isEmpty()) {
				values.remove(value);
				if(log.isDebugEnabled()) {
					log.debug("{} no longer exists in any row for column {}",
							value, name);
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Return the rows that satisfy {@code operator} in relation to
	 * {@code value}.
	 * 
	 * @param operator
	 * @param values
	 * @return the row set.
	 */
	public RowSet select(SelectOperator operator, Value... values) {
		RowSet results = null;
		switch (operator) {
		case BETWEEN:
			results = select.between(values[0], values[1]);
			break;
		case CONTAINS:
			results = select.contains(values[0]);
			break;
		case EQUALS:
			results = select.equal(values[0]);
			break;
		case NOT_EQUALS:
			results = select.notEquals(values[0]);
			break;
		case GREATER_THAN:
			results = select.greaterThan(values[0]);
			break;
		case GREATER_THAN_OR_EQUALS:
			results = select.greaterThanOrEquals(values[0]);
			break;
		case LESS_THAN:
			results = select.lessThan(values[0]);
			break;
		case LESS_THAN_OR_EQUALS:
			results = select.lessThanOrEquals(values[0]);
			break;
		case REGEX:
			results = select.regex(values[0]);
			break;
		case NOT_REGEX:
			results = select.notRegex(values[0]);
			break;
		}
		return results;
	}

	/**
	 * Remove {@code row} from any existing values and add {@code row}
	 * to {@code value}.
	 * 
	 * @param row
	 * @param value
	 * @return {@code true} if {@code row} is set.
	 */
	public boolean set(Key row, Value value) {
		Iterator<Value> it = values.keySet().iterator();
		while (it.hasNext()) { // iterate through every value in the column and
								// try to remove row
			remove(row, it.next());
		}
		return add(row, value);
	}

	/**
	 * A naturally sorted collection of {@link Key}.
	 */
	public static class RowSet extends TreeSet<Key> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * Return an empty row set.
		 * 
		 * @return the row set.
		 */
		public static RowSet createEmpty() {
			return new RowSet();
		}

		/**
		 * Construct a new instance.
		 */
		private RowSet() {}

	}

	/**
	 * A class that performs {@link #select} logic.
	 */
	private class Select {

		/**
		 * Perform a {@link SelectOperator#BETWEEN} select.
		 * 
		 * @param v1
		 * @param v2
		 * @return the result set.
		 */
		public RowSet between(Value v1, Value v2) {
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values
					.subMap(v1, true, v2, false).entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#CONTAINS} select.
		 * 
		 * @param query
		 * @return the result set.
		 */
		public RowSet contains(Value v) {
			RowSet result = RowSet.createEmpty();
			result.addAll(searcher.search(v.toString(), name));
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#EQUALS} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet equal(Value v) {
			return values.containsKey(v) ? values.get(v) : RowSet.createEmpty();
		}

		/**
		 * Perform a {@link SelectOperator#GREATER_THAN} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet greaterThan(Value v) {
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.tailMap(v, false)
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#GREATER_THAN_OR_EQUALS} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet greaterThanOrEquals(Value v) {
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.tailMap(v, true)
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#LESS_THAN} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet lessThan(Value v) {
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.headMap(v, false)
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#LESS_THAN_OR_EQUALS} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet lessThanOrEquals(Value v) {
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.headMap(v, true)
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#NOT_EQUALS} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet notEquals(Value v) {
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, RowSet> entry = it.next();
				if(!entry.getKey().equals(v)) {
					result.addAll(entry.getValue());
				}
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#NOT_REGEX} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet notRegex(Value v) {
			String regex = v.toString();
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, RowSet> entry = it.next();
				Pattern p = Pattern.compile(regex);
				Matcher m = p.matcher(entry.getValue().toString());
				if(!m.matches()) {
					result.addAll(entry.getValue());
				}
			}
			return result;
		}

		/**
		 * Perform a {@link SelectOperator#REGEX} select.
		 * 
		 * @param v
		 * @return the result set.
		 */
		public RowSet regex(Value v) {
			String regex = v.toString();
			RowSet result = RowSet.createEmpty();
			Iterator<Entry<Value, RowSet>> it = values.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Value, RowSet> entry = it.next();
				Pattern p = Pattern.compile(regex);
				Matcher m = p.matcher(entry.getValue().toString());
				if(!m.matches()) {
					result.addAll(entry.getValue());
				}
			}
			return result;
		}
	}

}
