package com.cinchapi.concourse.model;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;

import static org.junit.Assert.*;

/**
 * Implementation of the {@link Concourse} interface that provides abstractions
 * for common operations.
 * 
 * @author jnelson
 * 
 */
public abstract class AbstractConcourse implements Concourse {

	@Override
	public final boolean add(UnsignedLong row, String column, Object value) {
		assertNotNull(row);
		assertNotNull(column);
		assertNotNull(value);

		UnsignedLong timestamp = UnsignedLong.valueOf(System
				.currentTimeMillis());
		ConcourseValue _value = new ConcourseValue(value, timestamp);

		return addSpi(row, column, _value);
	}

	/**
	 * Uses a {@link ConcourseValue} to implement the interface for
	 * {@link #add(UnsignedLong, String, Object)}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if <code>value</code> is added.
	 */
	protected abstract boolean addSpi(UnsignedLong row, String column,
			ConcourseValue value);

	@Override
	public final boolean exists(UnsignedLong row, String column, Object value) {
		ConcourseValue _value = new ConcourseValue(value);
		return existsSpi(row, column, _value);
	}

	/**
	 * Implement the {@link #exists(UnsignedLong, String, Object)} interface
	 * using a {@link ConcourseValue}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if the cell exists.
	 */
	public abstract boolean existsSpi(UnsignedLong row, String column,
			ConcourseValue value);

	@Override
	public final Set<Object> get(UnsignedLong row, String column) {
		Set<ConcourseValue> _values = getSpi(row, column);

		Set<Object> values = Sets.newTreeSet(new Comparator<Object>() {

			@Override
			public int compare(Object o1, Object o2) {
				ConcourseValue v1 = (ConcourseValue) o1;
				ConcourseValue v2 = (ConcourseValue) o2;
				return -1 * v1.timestamp.compareTo(v2.timestamp);
			}

		});

		for (ConcourseValue _value : _values) {
			values.add(_value.value);
		}

		return values;
	}

	/**
	 * Return a set of {@link ConcourseValue} to implement the interface for
	 * {@link #get(UnsignedLong, String)}.
	 * 
	 * @param row
	 * @param column
	 * @return
	 */
	protected abstract Set<ConcourseValue> getSpi(UnsignedLong row,
			String column);

	@Override
	public final boolean remove(UnsignedLong row, String column, Object value) {
		ConcourseValue _value = new ConcourseValue(value);
		return removeSpi(row, column, _value);
	}

	/**
	 * Uses a {@link ConcourseValue} to implement the interface for
	 * {@link #remove(UnsignedLong, String, Object)}. The implemention should
	 * not depend on having the <code>timestamp</code> associated with the
	 * stored value.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if <code>value</code> is removed.
	 */
	protected abstract boolean removeSpi(UnsignedLong row, String column,
			ConcourseValue value);

	@Override
	public final Set<UnsignedLong> select(String column, Operator operator,
			Object... values) {
		List<ConcourseValue> _values = Lists
				.newArrayListWithCapacity(values.length);

		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			ConcourseValue _value = new ConcourseValue(value);
			_values.add(_value);
		}

		return selectSpi(column, operator, _values);
	}

	/**
	 * Implement the {@link #select(String, Operator, Object...)} interface with
	 * a list of {@link ConcourseValue}.
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the set of rows.
	 */
	protected abstract Set<UnsignedLong> selectSpi(String column,
			Operator operator, List<ConcourseValue> values);

	@Override
	public final boolean set(UnsignedLong row, String column, Object value) {
		Set<Object> values = get(row, column);
		Iterator<Object> it = values.iterator();
		while (it.hasNext()) {
			remove(row, column, it.next());
		}

		return add(row, column, value);
	}

	/**
	 * Provides an abstraction for all values stored within Concourse. All type
	 * information is handled within this class.
	 */
	@Immutable
	public static final class ConcourseValue {

		/**
		 * Return the <code>value</code> type.
		 * 
		 * @param value
		 * @return the type of <code>value</code>.
		 */
		public static Type determineType(Object value) {
			Type type;

			if(value instanceof Number) {
				type = Type.NUMBER;
			}
			else if(value instanceof Boolean) {
				type = Type.BOOLEAN;
			}
			else {
				type = Type.STRING;
			}

			return type;
		}

		@NotNull
		private final Object value;
		@NotNull
		private final Type type;
		@NotNull
		private final UnsignedLong timestamp;

		/**
		 * Construct a new instance without specifying a timestamp. This is
		 * useful for non-mutating callers (i.e. I want to check if a value
		 * exists as opposed to adding a new value) because these calls typicall
		 * use the {@link #equals(Object)} method, which ignores the timestamp.
		 * 
		 * @param value
		 * @param type
		 */
		public ConcourseValue(Object value) {
			this(value, UnsignedLong.valueOf(0L));
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param value
		 * @param type
		 * @param timestamp
		 */
		public ConcourseValue(Object value, UnsignedLong timestamp) {
			assertNotNull(value);
			assertNotNull(timestamp);

			this.value = value;
			this.type = determineType(value);
			this.timestamp = timestamp;
		}

		@Override
		public boolean equals(Object obj) {
			if(this == obj)
				return true;
			if(obj == null)
				return false;
			if(getClass() != obj.getClass())
				return false;
			ConcourseValue other = (ConcourseValue) obj;
			if(type != other.type)
				return false;
			if(value == null) {
				if(other.value != null)
					return false;
			}
			else if(!value.equals(other.value))
				return false;
			return true;
		}

		/**
		 * Return the <code>timestamp</code>.
		 * 
		 * @return the <code>timestamp</code>
		 */
		public UnsignedLong getTimestamp() {
			return timestamp;
		}

		/**
		 * Return the <code>type</code>.
		 * 
		 * @return the <code>type</code>.
		 */
		public String getType() {
			return this.type.toString();
		}

		/**
		 * Return the <code>value</code>.
		 * 
		 * @return the <code>value</code>.
		 */
		public Object getValue() {
			return value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		/**
		 * Return <code>value.toString()</code>.
		 */
		@Override
		public String toString() {
			return value.toString();
		}

		public enum Type {
			NUMBER, BOOLEAN, STRING;

			@Override
			public String toString() {
				return this.name().toLowerCase();
			}
		}

	}

}
