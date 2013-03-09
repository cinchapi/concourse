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
package com.cinchapi.concourse.api;

import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * <p>Provides base functionality that is common to every {@link DataStoreService}.</p>
 * <p><strong>Note:</strong> The implementing class is responsible for all necessary
 * synchronization/locking.</p>
 * 
 * @author Jeff Nelson
 */
public abstract class ConcourseService implements DataStoreService {

	@Override
	public final boolean add(long row, String column, Object value) {
		Preconditions.checkArgument(
				!com.cinchapi.common.Strings.containsWhitespace(column),
				"The column name cannot contain whitespace");
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
		return !get(row, column).isEmpty();
	}

	@Override
	public final boolean exists(long row, String column, Object value) {
		return existsSpi(row, column, value);
	}

	@Override
	public final Set<Object> get(long row, String column) {
		return getSpi(row, column);
	}

	@Override
	public final boolean remove(long row, String column, Object value) {
		return removeSpi(row, column, value);
	}

	@Override
	public final Set<Long> select(String column, SelectOperator operator,
			Object... values) {
		return selectSpi(column, operator, values);
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
	 * Implement the interface for {@link #get(long, String)}.
	 * 
	 * @param row
	 * @param column
	 * @return the set of values that currently exist in the cell located at the
	 *         intersection of {@code row} and {@code column}
	 */
	protected abstract Set<Object> getSpi(long row, String column);

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
	 * Implement the interface for
	 * {@link #select(String, SelectOperator, Object...)}.
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the set of rows that match the select criteria
	 */
	protected abstract Set<Long> selectSpi(String column,
			SelectOperator operator, Object... values);

	/**
	 * Default implement of the interface for {@code #set(long, String, Object)}
	 * . Feel free to override if necessary.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if the set is successful
	 */
	protected boolean setSpi(long row, String column, Object value) {
		Set<Object> values = get(row, column);
		for (Object v : values) {
			remove(row, column, v);
		}
		return add(row, column, value);
	}

}
