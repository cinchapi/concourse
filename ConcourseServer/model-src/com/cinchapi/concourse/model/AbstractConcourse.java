package com.cinchapi.concourse.model;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.jetbrains.annotations.NotNull;

import com.cinchapi.util.AtomicClock;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Implementation of the {@link Concourse} interface that provides abstractions
 * for common operations.
 * 
 * @author jnelson
 * 
 */
public abstract class AbstractConcourse implements Concourse {

	private static Gson json = new GsonBuilder().setPrettyPrinting().create();
	private static AtomicClock clock = new AtomicClock();

	@Override
	public final boolean add(UnsignedLong row, String column, Object value) {
		Preconditions.checkNotNull(row);
		Preconditions.checkNotNull(column);
		Preconditions.checkArgument(!column.contains(" "),
				"column name cannot contain spaces");
		Preconditions.checkNotNull(value);
		Preconditions.checkArgument(
				(value instanceof UnsignedLong && exists((UnsignedLong) value))
						|| !(value instanceof UnsignedLong), value
						+ " is not an existing row");

		UnsignedLong timestamp = clock.time();
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
		Set<Object> values = Sets.newLinkedHashSetWithExpectedSize(_values
				.size());
		for (ConcourseValue _value : _values) {
			values.add(_value.value);
		}

		return values;
	}

	/**
	 * Return a set of {@link ConcourseValue} sorted by timestamp to implement
	 * the interface for {@link #get(UnsignedLong, String)}.
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

	/**
	 * Return a the set of non-null rows.
	 * 
	 * @return the row set.
	 */
	protected abstract Set<UnsignedLong> rowSet();

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

	@Override
	public String toString() {
		JsonObject collection = new JsonObject();
		Iterator<UnsignedLong> rows = rowSet().iterator();
		while (rows.hasNext()) {
			UnsignedLong row = rows.next();
			JsonObject object = new JsonObject();
			Iterator<String> columns = describe(row).iterator();
			while (columns.hasNext()) {
				String column = columns.next();
				Set<ConcourseValue> values = getSpi(row, column);
				object.add(column, toStringExtractJsonElement(values));
			}
			collection.add(row.toString(), object);
		}
		return json.toJson(collection);
	}

	/**
	 * Extract a {@link JsonElement} from <code>values</code>
	 * 
	 * @param values
	 * @return the element.
	 */
	private JsonElement toStringExtractJsonElement(Set<ConcourseValue> values) {
		if(values.size() > 1) {
			JsonArray array = new JsonArray();
			Iterator<ConcourseValue> it = values.iterator();
			while (it.hasNext()) {
				array.add(toStringExtractJsonPrimitive(it.next()));
			}
			return array;
		}
		else {
			return toStringExtractJsonPrimitive(values.iterator().next());
		}
	}

	/**
	 * Extract a {@link JsonPrimitive} from <code>value</code>.
	 * 
	 * @param value
	 * @return the primitive.
	 */
	private JsonObject toStringExtractJsonPrimitive(ConcourseValue value) {
		JsonObject result = new JsonObject();
		switch (value.type) {
		case BOOLEAN:
			result.addProperty("value", ((Boolean) value.getValue()));
			break;
		case NUMBER:
			result.addProperty("value", ((Number) value.getValue()));
			break;
		case RELATION:
			result.addProperty("value", ((Number) value.getValue()));
			break;
		case STRING:
			result.addProperty("value", ((String) value.getValue()));
			break;
		}
		result.addProperty("type", value.getType());
		return result;
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
			if(value instanceof UnsignedLong) {
				type = Type.RELATION;
			}
			else if(value instanceof Number) {
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
		 * The empty timestamp used with {@link #AbstractConcourse(Object)}.
		 */
		protected static UnsignedLong EMPTY_TIMESTAMP = UnsignedLong
				.valueOf(0L);

		/**
		 * Construct a new instance without specifying a timestamp. This is
		 * useful for non-mutating callers (i.e. I want to check if a value
		 * exists as opposed to adding a new value) because these calls
		 * typically use the {@link #equals(Object)} method or an
		 * {@link Comparator} which ignores the timestamp.
		 * 
		 * @param value
		 * @param type
		 */
		public ConcourseValue(Object value) {
			this(value, EMPTY_TIMESTAMP);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param value
		 * @param type
		 * @param timestamp
		 */
		public ConcourseValue(Object value, UnsignedLong timestamp) {
			Preconditions.checkNotNull(value);
			Preconditions.checkNotNull(timestamp);

			this.value = value;
			this.timestamp = timestamp;
			this.type = determineType(value);

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
			NUMBER, BOOLEAN, RELATION, STRING;

			@Override
			public String toString() {
				return this.name().toLowerCase();
			}
		}

	}

}
