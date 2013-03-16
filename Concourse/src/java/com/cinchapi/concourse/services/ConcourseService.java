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
package com.cinchapi.concourse.services;

import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Provides base functionality that is common to every {@link DataStoreService}.
 * All bounds checking for parameters is handled here.
 * </p>
 * <p>
 * <strong>Note:</strong> The implementing class is responsible for all
 * necessary synchronization/locking.
 * </p>
 * 
 * @author jnelson
 */
public abstract class ConcourseService implements DataStoreService {

	/**
	 * Return {@code true} if {@code column} is a legal name. See
	 * {@link #ILLEGAL_COLUMN_NAME_CHARS} for details. This method will throw an
	 * exception if the column name is not legal.
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
	public final boolean add(long row, String column, Object value) {
		ConcourseService.checkColumnName(column);
		return addSpi(row, column, value);
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
	public final boolean exists(long row, String column) {
		return !fetch(row, column).isEmpty();
	}

	@Override
	public final boolean exists(long row, String column, Object value) {
		return existsSpi(row, column, value);
	}

	@Override
	public final Set<Object> fetch(long row, String column) {
		return fetchSpi(row, column);
	}

	@Override
	public final Set<Object> fetch(long row, String column, long timestamp) {
		// TODO this should be pretty easy for the permanent database, but might
		// be harder/slower for the commitlog
		throw new UnsupportedOperationException(
				"This operation is not currently supported");
	}

	@Override
	public final Set<Long> query(String column, Operator operator,
			Object... values) {
		return querySpi(column, operator, values);
	}

	@Override
	public final boolean remove(long row, String column, Object value) {
		return removeSpi(row, column, value);
	}

	@Override
	public final boolean revert(long row, String column, long timestamp) {
		// TODO figure out if this should erase history or create new
		// history...creating new history would be easier technically because
		// i could simply walk backwards and repeat commits but adding new
		// history while creating might be counter intuitive..

		throw new UnsupportedOperationException(
				"This operation is not currently supported");
	}

	@Override
	public final boolean set(long row, String column, Object value) {
		return setSpi(row, column, value);
	}

	/**
	 * Implement the interface for {@link #add(long, String, Object)}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if the add is successful
	 */
	protected abstract boolean addSpi(long row, String column, Object value);

	/**
	 * Implement the interface for {@link #describe(long)}.
	 * 
	 * @param row
	 * @return the set of columns in {@code row}
	 */
	protected abstract Set<String> describeSpi(long row);

	/**
	 * Implement the interface for {@link #exists(long, String, Object)}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} exists in the cell located at the
	 *         intersection of {@code row} and {@code column}
	 */
	protected abstract boolean existsSpi(long row, String column, Object value);

	/**
	 * Implement the interface for {@link #fetch(long, String)}.
	 * 
	 * @param row
	 * @param column
	 * @return the set of values that currently exist in the cell located at the
	 *         intersection of {@code row} and {@code column}
	 */
	protected abstract Set<Object> fetchSpi(long row, String column);

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
	 * Implement the interface for {@link #remove(long, String, Object)}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if the remove is successful
	 */
	protected abstract boolean removeSpi(long row, String column, Object value);

	/**
	 * Default implementation of the interface for
	 * {@code #set(long, String, Object)}. Feel free to override if necessary.
	 * This implementation gets the values for the cell at [{@code row} x
	 * {@code column}] and removes them all. Afterwards it adds {@code value} in
	 * [{@code row} x {@code column}].
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if the set is successful
	 */
	protected boolean setSpi(long row, String column, Object value) {
		Set<Object> values = fetch(row, column);
		for (Object v : values) {
			remove(row, column, v);
		}
		return add(row, column, value);
	}

}
