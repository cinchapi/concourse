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

import com.cinchapi.concourse.db.Operator;

/**
 * A service that can handle reads.
 * 
 * @author jnelson
 */
public interface ReadService {

	/**
	 * Describe {@code row}.
	 * 
	 * @param row
	 * @return the set of non-empty columns in {@code row}
	 */
	public Set<String> describe(long row);

	/**
	 * Check if {@code column} exists as {@code value} for {@code row}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return {@code true} if the cell at {@code row} x {@code column} contains
	 *         {@code value}
	 */
	public boolean exists(String column, Object value, long row);

	/**
	 * Fetch {@code column} for {@code row}.
	 * 
	 * @param column
	 * @param row
	 * @return the set of {@code values} that exist in the cell at {@code row} x
	 *         {@code column}
	 */
	public Set<Object> fetch(String column, long row);

	/**
	 * Query {@code column} for the the rows that satisfy {@code operator} in
	 * relation to the appropriate number of {@code values}
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the result set
	 */
	public Set<Long> query(String column, Operator operator, Object... values);

}
