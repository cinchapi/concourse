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
package com.cinchapi.concourse.engine;

import java.util.Set;

/**
 * A service that can handle historical reads.
 * 
 * @author jnelson
 */
public interface HistoricalReadService {

	/**
	 * Describe {@code row} at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @param row
	 * @return the set of non-empty columns in {@code row} at {@code timestamp}.
	 */
	public Set<String> describeAt(long timestamp, long row);

	/**
	 * Check if {@code column} exists as {@code value} for {@code row} at
	 * {@code timestamp}.
	 * 
	 * @param timestamp
	 * @param column
	 * @param value
	 * @param row
	 * @return {@code true} if the cell at {@code row} x {@code column} contains
	 *         {@code value} at {@code timestamp}.
	 */
	public boolean existsAt(long timestamp, String column, Object value,
			long row);

	/**
	 * Fetch {@code column} for {@code row} at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @param column
	 * @param row
	 * @return the set of {@code values} that existed in the cell at {@code row}
	 *         x {@code column} at {@code timestamp}.
	 */
	public Set<Object> fetchAt(long timestamp, String column, long row);

	/**
	 * Query {@code column} for the the rows that satisfy {@code operator} in
	 * relation to the appropriate number of {@code values} at {@code timestamp}
	 * .
	 * 
	 * @param timestamp
	 * @param column
	 * @param operator
	 * @param values
	 * @return the result set at {@code timestamp}.
	 */
	public Set<Long> queryAt(long timestamp, String column, Operator operator,
			Object... values);

}
