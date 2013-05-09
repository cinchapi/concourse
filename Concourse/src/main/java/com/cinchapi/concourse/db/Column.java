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
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.cinchapi.concourse.db.Operator.*;

/**
 * <p>
 * A Column is an inverted index {@link BucketMap} that maps a Value to a collection
 * of primary keys. This structure is designed to efficiently answer most query
 * reads.
 * </p>
 * 
 * @author jnelson
 */
final class Column extends BucketMap<Value, PrimaryKey> {

	/**
	 * Return the column that is located by {@code name}.
	 * 
	 * @param name
	 * @return the column
	 */
	public static Column fromName(ByteSizedString name) {
		Column column = cache.get(name);
		if(column == null) {
			column = new Column(name);
			cache.put(column, name);
		}
		return column;
	}

	private static final Cell mock = Bucket.mock(Cell.class);
	private static final ObjectReuseCache<Column> cache = new ObjectReuseCache<Column>();
	protected static final String FILE_NAME_EXT = "ccc"; // @Override from
															// {@link Store}

	/**
	 * Construct a new instance.
	 * 
	 * @param locator
	 */
	private Column(ByteSizedString name) {
		super(name);
	}

	@Override
	protected Bucket<Value, PrimaryKey> getBucketFromByteSequence(
			ByteBuffer bytes) {
		return new Cell(bytes);
	}

	@Override
	protected Bucket<Value, PrimaryKey> getMockBucket() {
		return mock;
	}

	@Override
	protected Bucket<Value, PrimaryKey> getNewBucket(Value value) {
		return new Cell(value);
	}

	@Override
	protected Map<Value, Bucket<Value, PrimaryKey>> getNewBuckets(
			int expectedSize) {
		return Maps.newTreeMap(new ValueComparator());
	}

	/**
	 * Return the rows that satisfy {@code operator} in relation to the
	 * specified {@code values}.
	 * 
	 * @param operator
	 * @param values
	 * @return the set of rows that match the query
	 */
	Set<PrimaryKey> query(Operator operator, Value... values) {
		return query(false, 0, operator, values);
	}

	/**
	 * Return the rows that satisfy {@code operator} in relation to the
	 * specified {@code values} at the specified {@code timestamp}.
	 * 
	 * @param timestamp
	 * @param operator
	 * @param values
	 * @return the set of rows that match the query
	 */
	Set<PrimaryKey> query(long timestamp, Operator operator, Value... values) {
		return query(true, timestamp, operator, values);
	}

	/**
	 * Return the rows that satisfy {@code operator} in relation to the
	 * specified {@code values} at the present or at the specified
	 * {@code timestamp} if {@code historical is {@code true}
	 * 
	 * @param historical - if {@code true} query the history for each cell,
	 *            otherwise query the present state
	 * @param timestamp - this value is ignored if {@code historical} is set to
	 *            false, otherwise this value is the historical timestamp at
	 *            which to query each cell
	 * @param operator
	 * @param values
	 * @return the set of rows that match the query
	 */
	private Set<PrimaryKey> query(boolean historical, long timestamp,
			Operator operator, Value... values) {
		Set<PrimaryKey> keys = Sets.newTreeSet();
		Value value = values[0];

		if(operator == EQUALS) {
			keys.addAll(historical ? get(value).getValues(timestamp) : get(
					value).getValues());
		}
		else if(operator == NOT_EQUALS) {
			Iterator<Bucket<Value, PrimaryKey>> it = buckets().values()
					.iterator();
			while (it.hasNext()) {
				Bucket<Value, PrimaryKey> bucket = it.next();
				if(!bucket.getKey().equals(value)) {
					keys.addAll(historical ? bucket.getValues(timestamp)
							: bucket.getValues());
				}
			}
		}
		else if(operator == GREATER_THAN) {
			Iterator<Bucket<Value, PrimaryKey>> it = ((TreeMap<Value, Bucket<Value, PrimaryKey>>) buckets())
					.tailMap(value, false).values().iterator();
			while (it.hasNext()) {
				keys.addAll(historical ? it.next().getValues(timestamp) : it
						.next().getValues());
			}
		}
		else if(operator == GREATER_THAN_OR_EQUALS) {
			Iterator<Bucket<Value, PrimaryKey>> it = ((TreeMap<Value, Bucket<Value, PrimaryKey>>) buckets())
					.tailMap(value, true).values().iterator();
			while (it.hasNext()) {
				keys.addAll(historical ? it.next().getValues(timestamp) : it
						.next().getValues());
			}
		}
		else if(operator == LESS_THAN) {
			Iterator<Bucket<Value, PrimaryKey>> it = ((TreeMap<Value, Bucket<Value, PrimaryKey>>) buckets())
					.headMap(value, false).values().iterator();
			while (it.hasNext()) {
				keys.addAll(historical ? it.next().getValues(timestamp) : it
						.next().getValues());
			}
		}
		else if(operator == GREATER_THAN_OR_EQUALS) {
			Iterator<Bucket<Value, PrimaryKey>> it = ((TreeMap<Value, Bucket<Value, PrimaryKey>>) buckets())
					.headMap(value, true).values().iterator();
			while (it.hasNext()) {
				keys.addAll(historical ? it.next().getValues(timestamp) : it
						.next().getValues());
			}
		}
		else if(operator == BETWEEN) {
			Preconditions.checkArgument(values.length > 1,
					"You must specify two arguments for the BETWEEN operator.");
			Value value2 = values[1];
			Iterator<Bucket<Value, PrimaryKey>> it = ((TreeMap<Value, Bucket<Value, PrimaryKey>>) buckets())
					.subMap(value, true, value2, false).values().iterator();
			while (it.hasNext()) {
				keys.addAll(historical ? it.next().getValues(timestamp) : it
						.next().getValues());
			}
		}
		else if(operator == REGEX) {
			Iterator<Bucket<Value, PrimaryKey>> it = buckets().values()
					.iterator();
			while (it.hasNext()) {
				Bucket<Value, PrimaryKey> bucket = it.next();
				Pattern p = Pattern.compile(value.getQuantity().toString());
				Matcher m = p.matcher(bucket.getKey().toString());
				if(m.matches()) {
					keys.addAll(historical ? it.next().getValues(timestamp)
							: it.next().getValues());
				}
			}
		}
		else if(operator == NOT_REGEX) {
			Iterator<Bucket<Value, PrimaryKey>> it = buckets().values()
					.iterator();
			while (it.hasNext()) {
				Bucket<Value, PrimaryKey> bucket = it.next();
				Pattern p = Pattern.compile(value.getQuantity().toString());
				Matcher m = p.matcher(bucket.getKey().toString());
				if(!m.matches()) {
					keys.addAll(historical ? it.next().getValues(timestamp)
							: it.next().getValues());
				}
			}
		}
		else {
			throw new UnsupportedOperationException(operator
					+ " operator is unsupported");
		}
		return keys;
	}

	/**
	 * <p>
	 * The bucketed view of stored data from the perspective of a {@link Column}
	 * that is designed to efficiently handle query reads.
	 * </p>
	 * 
	 * @author jnelson
	 */
	final static class Cell extends Bucket<Value, PrimaryKey> {

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

		/**
		 * Construct a new instance.
		 * 
		 * @param key
		 */
		Cell(Value key) {
			super(key);
		}

		@Override
		protected Value getKeyFromByteSequence(ByteBuffer bytes) {
			return Value.fromByteSequence(bytes);
		}

		@Override
		protected PrimaryKey getValueFromByteSequence(ByteBuffer bytes) {
			return PrimaryKey.fromByteSequence(bytes);
		}

	}

}
