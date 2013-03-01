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
package com.cinchapi.concourse.search;

import java.util.Set;

import com.cinchapi.concourse.db.Key;

/**
 * A service that searches a fulltext index.
 * 
 * @author jnelson
 */
public interface Searcher {

	/**
	 * Return the set of {@code keys} for the rows containing
	 * {@code query} in any column.
	 * 
	 * @param query
	 * @return the result set.
	 */
	public Set<Key> search(String query);

	/**
	 * Return the set of {@code keys} for the rows containing
	 * {@code query} in {@code column}.
	 * 
	 * @param query
	 * @return the result set.
	 */
	public Set<Key> search(String query, String column);

	/**
	 * Return the set of {@code columns} in {@code row} that contain
	 * {@code query}.
	 * 
	 * @param query
	 * @return the result set.
	 */
	public Set<String> search(String query, Key row);

}
