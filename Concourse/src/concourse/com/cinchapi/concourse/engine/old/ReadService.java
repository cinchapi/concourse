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

/**
 * A service that can read stored data.
 * 
 * @author jnelson
 */
public interface ReadService {

	/**
	 * Return a list of columns in {@code row}.
	 * 
	 * @param row
	 * @return the set of {@code non-null} columns in {@code row}. An empty
	 *         return value indicates that {@link #exists(long)} for {@code row}
	 *         is {@code false}
	 */
	public Set<String> describe(long row);

	/**
	 * Check if {@code row} exists.
	 * 
	 * @param row
	 * @return {@code true} if {@link #describe(long)} for {@code row} is a
	 *         non-empty set
	 */
	public boolean exists(long row);

	/**
	 * Check if {@code column} exists in {@code row}.
	 * 
	 * @param column
	 * @param row
	 * 
	 * @return {@code true} if {@link #fetch(String, long)} for {@code row} and
	 *         {@code column} is not empty
	 */
	public boolean exists(String column, long row);

	/**
	 * Check if {@code column} as {@code value} exists in {@code row}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if {@code value} is contained in the cell at
	 *         {@code row}:{@code column}
	 */
	public boolean exists(String column, Object value, long row);

	/**
	 * Fetch {@code column} in {@code row}.
	 * 
	 * @param column
	 * @param row
	 * 
	 * @return the values in the cell at {@code row}:{@code column}
	 */
	public Set<Object> fetch(String column, long row);

	/**
	 * Fetch {@code column} at {@code timestamp} in {@code row}.
	 * 
	 * @param column
	 * @param row
	 * @param at
	 * 
	 * @return the values in the cell at {@code row}:{@code column} as existed
	 *         at {@code timestamp}
	 */
	public Set<Object> fetch(String column, long timestamp, long row);

}
