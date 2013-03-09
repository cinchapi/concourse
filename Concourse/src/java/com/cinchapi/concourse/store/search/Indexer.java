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
package com.cinchapi.concourse.store.search;

import com.cinchapi.concourse.store.perm.Key;
import com.cinchapi.concourse.store.perm.Value;

/**
 * A service that indexes data for fulltext searching.
 * 
 * @author jnelson
 */
public interface Indexer {

	/**
	 * Remove {@code value} from the indexes for {@code row} and
	 * {@code column}
	 * 
	 * @param row
	 * @param column
	 * @param value
	 */
	public void deindex(Key row, String column, Value value);

	/**
	 * Index {@code value} to {@code row} and {@code column} for
	 * fulltext searching.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 */
	public void index(Key row, String column, Value value);

}
