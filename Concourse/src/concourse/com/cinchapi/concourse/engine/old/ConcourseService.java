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

import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.db.Key;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * <p>
 * Provides base functionality for every {@link DataStoreService}. All bounds
 * checking for parameters is handled here so extending classes are only
 * responsible for providing method logic.
 * </p>
 * <p>
 * <strong>Note:</strong> The implementing class is responsible for concurrency
 * control.
 * </p>
 * 
 * @author jnelson
 */
public abstract class ConcourseService implements DataStoreService {

	/**
	 * Return {@code true} if {@code column} is a legal name. See
	 * {@link #ILLEGAL_COLUMN_NAME_CHARS} for details. This method will throw an
	 * exception if the column name is illegal.
	 * 
	 * @param column
	 * @return {@code true} if the name is legal
	 * @throws IllegalArgumentException
	 */
	public static boolean checkColumnName(String column)
			throws IllegalArgumentException {
		Preconditions.checkArgument(
				!com.cinchapi.common.Strings.containsWhitespace(column),
				"The column name cannot contain whitespace");
		Preconditions
				.checkArgument(
						!com.cinchapi.common.Strings.contains(column,
								ILLEGAL_COLUMN_NAME_CHARS),
						"The column name connot contained any of the following banned characters : %s",
						(Object[]) ILLEGAL_COLUMN_NAME_CHARS);
		for (Operator operator : Operator.values()) {
			Preconditions
					.checkArgument(
							!column.matches("(?i).*" + operator + ".*"),
							"The column name cannot contain a substring of %s because it is reserved word",
							operator);
		}
		return true;
	}

	/**
	 * The list of characters that cannot appear in a legal column name. The
	 * list includes:
	 * <ul>
	 * <li>comma (,)</li>
	 * <li>double quote (")</li>
	 * <li>single quote (')</li>
	 * <li>back slash (\)</li>
	 * <li>open parenthesis (()</li>
	 * <li>close parenthesis ())</li>
	 * </ul>
	 */
	public static final CharSequence[] ILLEGAL_COLUMN_NAME_CHARS = { ",", "\"",
			"'", "\\", "(", ")" };

	@Override
	public final boolean add(String column, Object value, long row) {
		if(!exists(column, value, row) && isValidValue(value)) {
			ConcourseService.checkColumnName(column);
			return addSpi(column, value, row);
		}
		return false;
	}

	@Override
	public final Set<String> describe(long row) {
		return describeSpi(row);
	}

	@Override
	public final boolean exists(long row) {
		return !describe(row).isEmpty();
	}

	@Override
	public final boolean exists(String column, long row) {
		return !fetch(column, row).isEmpty();
	}

	@Override
	public final boolean exists(String column, Object value, long row) {
		return existsSpi(column, value, row);
	}

	@Override
	public Set<Object> fetch(String column, long row) {
		return fetch(column, Time.now(), row);
	}

	@Override
	public final Set<Object> fetch(String column, long timestamp, long row) {
		return fetchSpi(column, timestamp, row);
	}

	@Override
	public final Set<Long> query(String column, Operator operator,
			Object... values) {
		return querySpi(column, operator, values);
	}

	@Override
	public final Set<Long> query(Set<Object> within, String column,
			Operator operator, Object... values) {
		Set<Long> all = query(column, operator, values);
		@SuppressWarnings({ "unchecked", "rawtypes" })// See
		// http://stackoverflow.com/a/1177788/1336833. This conversion is okay
		// because the intersection of the sets can only possibly contain longs
		Set<Long> intersection = (Set) Sets.intersection(within, all);
		return intersection;
	}

	@Override
	public final boolean remove(String column, Object value, long row) {
		if(exists(column, value, row)) {
			return removeSpi(column, value, row);
		}
		return false;
	}

	@Override
	public final boolean revert(String column, long timestamp, long row) {
		Set<Object> past = fetch(column, timestamp, row);
		Set<Object> present = fetch(column, row);
		Set<Object> xor = Sets.symmetricDifference(past, present);
		for (Object value : xor) {
			if(present.contains(value)) {
				remove(column, value, row);
			}
			else {
				add(column, value, row);
			}
		}
		return Sets.symmetricDifference(fetch(column, row),
				fetch(column, timestamp, row)).isEmpty();
	}

	@Override
	public final boolean set(String column, Object value, long row) {
		Set<Object> values = fetch(column, row);
		for (Object v : values) {
			remove(column, v, row);
		}
		return add(column, value, row);
	}

	@Override
	public long sizeOf() {
		return sizeOf(null, null);
	}

	@Override
	public long sizeOf(long row) {
		return sizeOf(null, row);
	}

	@Override
	public long sizeOf(String column, Long row) {
		Preconditions
				.checkArgument(
						row == null && column == null,
						"Calculating the size of a column is not supported. "
								+ "If the row parameter is null, the column parameter must also be null.");
		return sizeOfSpi(column, row);
	}

	/**
	 * Implement the interface for {@link #add(String, Object, long)}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if the add is successful
	 */
	protected abstract boolean addSpi(String column, Object value, long row);

	/**
	 * Implement the interface for {@link #describe(long)}.
	 * 
	 * @param row
	 * @return the set of columns in {@code row}
	 */
	protected abstract Set<String> describeSpi(long row);

	/**
	 * Implement the interface for {@link #exists(String, Object, long)}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if {@code value} exists in the cell located at the
	 *         intersection of {@code row} and {@code column}
	 */
	protected abstract boolean existsSpi(String column, Object value, long row);

	/**
	 * Implement the interface for {@link #fetch(String, long, long)}.
	 * 
	 * @param column
	 * @param timestamp
	 * @param row
	 * 
	 * @return the set of values that exist in the cell located at the
	 *         intersection of {@code row} and {@code column} at
	 *         {@code timestamp}
	 */
	protected abstract Set<Object> fetchSpi(String column, long timestamp,
			long row);

	/**
	 * Implement the interface for {@link #query(String, Operator, Object...)}.
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the set of rows that match the select criteria
	 */
	protected abstract Set<Long> querySpi(String column, Operator operator,
			Object... values);

	/**
	 * Implement the interface for {@link #remove(String, Object, long)}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if the remove is successful
	 */
	protected abstract boolean removeSpi(String column, Object value, long row);

	/**
	 * <p>
	 * Return the size in bytes.
	 * </p>
	 * <p>
	 * <ul>
	 * <li>Implement the interface for {@link #sizeOf(String, Long)} if
	 * {@code row} and {@code column} both != {@code null}.</li>
	 * <li>Implement the interface for {@link #sizeOf(Long)} if {@code row} ==
	 * {@code null}.</li>
	 * <li>Implement the interface for {@link #sizeOf()} if {@code row} and
	 * {@code column} both == {@code null}.</li>
	 * </ul>
	 * </p>
	 * 
	 * @param column
	 * @param row
	 * 
	 * @return the size in bytes
	 */
	protected abstract long sizeOfSpi(@Nullable String column,
			@Nullable Long row);

	/**
	 * Check to see if {@code value} is valid. This method will throw an
	 * {@link IllegalArgumentException} if the value is not valid.
	 * 
	 * @param value
	 * @return {@code true} if the value is valid
	 * @throws IllegalArgumentException
	 */
	private boolean isValidValue(Object value) throws IllegalArgumentException {
		Preconditions
				.checkArgument(
						!(value instanceof Key)
								|| (value instanceof Key && exists(((Key) value)
										.asLong())),
						"Cannot add a relation to row %s because that row does not exist",
						value);
		return true;
	}

}
