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
package com.cinchapi.concourse.db.services;

import java.util.Set;

import com.cinchapi.concourse.db.PrimaryKey;
import com.cinchapi.concourse.db.Operator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * A highly available service that handles the physical storage and retrieval of
 * data.
 * 
 * @author jnelson
 */
public abstract class DataStoreService implements
		WriteService,
		ReadService,
		HistoryService {

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
				!com.cinchapi.common.util.Strings.containsWhitespace(column),
				"The column name cannot contain whitespace");
		Preconditions
				.checkArgument(
						!com.cinchapi.common.util.Strings.contains(column,
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
	public boolean add(String column, Object value, long row) {
		if(!exists(column, value, row) && isValidValue(value)) {
			checkColumnName(column);
			return addSpi(column, value, row);
		}
		return false;
	}

	@Override
	public Set<String> describe(long row) {
		return describeSpi(row);
	}

	@Override
	public Set<String> describeAt(long timestamp, long row) {
		return describeAtSpi(timestamp, row);
	}

	@Override
	public boolean exists(String column, Object value, long row) {
		return existsSpi(column, value, row);
	}

	@Override
	public boolean existsAt(long timestamp, String column, Object value,
			long row) {
		return existsAtSpi(timestamp, column, value, row);
	}

	@Override
	public Set<Object> fetch(String column, long row) {
		return fetchSpi(column, row);
	}

	@Override
	public Set<Object> fetchAt(long timestamp, String column, long row) {
		return fetchAtSpi(timestamp, column, row);
	}

	@Override
	public Set<Long> query(String column, Operator operator, Object... values) {
		return querySpi(column, operator, values);
	}

	@Override
	public Set<Long> queryAt(long timestamp, String column, Operator operator,
			Object... values) {
		return queryAtSpi(timestamp, column, operator, values);
	}

	@Override
	public boolean remove(String column, Object value, long row) {
		if(exists(column, value, row)) {
			return removeSpi(column, value, row);
		}
		return false;
	}

	@Override
	public final boolean revert(String column, long row, long timestamp) {
		Set<Object> past = fetchAt(timestamp, column, row);
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
				fetchAt(timestamp, column, row)).isEmpty();
	}

	/**
	 * Implement the interface for {@link #add(String, Object, long)}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract boolean addSpi(String column, Object value, long row);

	/**
	 * Implement the interface for {@link #describeAt(long, long)}.
	 * 
	 * @param timestamp
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract Set<String> describeAtSpi(long timestamp, long row);

	/**
	 * Implement the interface for {@link #describe(long)}.
	 * 
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract Set<String> describeSpi(long row);

	/**
	 * Return {@code true} if {@code row} has at least one non-empty cell.
	 * 
	 * @param row
	 * @return {@code true} if {@code row} exists
	 */
	protected boolean exists(long row) {
		return !describe(row).isEmpty();
	}

	/**
	 * Implement the interface for {@link #existsAt(long, String, Object, long)}
	 * .
	 * 
	 * @param timestamp
	 * @param column
	 * @param value
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract boolean existsAtSpi(long timestamp, String column,
			Object value, long row);

	/**
	 * Implement the interface for {@link #exists(String, Object, long)}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract boolean existsSpi(String column, Object value, long row);

	/**
	 * Implement the interface for {@link #fetchAt(long, String, long)}.
	 * 
	 * @param timestamp
	 * @param column
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract Set<Object> fetchAtSpi(long timestamp, String column,
			long row);

	/**
	 * Implement the interface for {@link #fetch(String, long)}.
	 * 
	 * @param column
	 * @param row
	 * @return the result of the operation
	 */
	protected abstract Set<Object> fetchSpi(String column, long row);

	/**
	 * Implement the interface for
	 * {@link #queryAt(long, String, Operator, Object...)}.
	 * 
	 * @param timestamp
	 * @param column
	 * @param operator
	 * @param values
	 * @return the result of the operation
	 */
	protected abstract Set<Long> queryAtSpi(long timestamp, String column,
			Operator operator, Object... values);

	/**
	 * Implement the interface for {@link #query(String, Operator, Object...)}.
	 * 
	 * @param string
	 * @param operator
	 * @param values
	 * @return the result of the operation
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
	 * Check to see if {@code value} is valid.
	 * 
	 * @param value
	 * @return {@code true} if the value is valid
	 * @throws IllegalArgumentException
	 */
	private boolean isValidValue(Object value) throws IllegalArgumentException {
		Preconditions
				.checkArgument(
						!(value instanceof PrimaryKey)
								|| (value instanceof PrimaryKey && exists(((PrimaryKey) value)
										.asLong())),
						"Cannot add a relation to row %s because that row does not exist",
						value);
		return true;
	}

}
