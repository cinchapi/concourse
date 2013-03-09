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
package com.cinchapi.concourse.store.api;

import java.util.Set;

/**
 * Specifies the possible ways in which data can be retrieved from a {@code row}
 * and/or {@code column}.
 * 
 * @author jnelson
 */
public interface Retrievable {

	/**
	 * Return a list of columns in {@code row}.
	 * 
	 * @param row
	 * @return the set of {@code non-null} columns in {@code row}. An empty
	 *         return value indicates that {@link #exists(long)} for {@code row}
	 *         is {@code false}.
	 */
	public Set<String> describe(long row);

	/**
	 * Return {@code true} if {@code row} exists.
	 * 
	 * @param row
	 * @return {@code true} if {@link #describe(long)} for {@code row} is not
	 *         empty.
	 */
	public boolean exists(long row);

	/**
	 * Return {@code true} if {@code column} exists in {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @return {@code true} if {@link #get(long, String)} for {@code row} and
	 *         {@code column} is not empty.
	 */
	public boolean exists(long row, String column);

	/**
	 * Return {@code true} if {@code row} contains {@code value} under
	 * {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is contained.
	 */
	public boolean exists(long row, String column, Object value);

	/**
	 * Return the values in {@code column} for {@code row} sorted by timestamp
	 * in descending order.
	 * 
	 * @param row
	 * @param column
	 * @return the result set.
	 */
	public Set<Object> get(long row, String column);

}
