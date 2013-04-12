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

/**
 * A highly available distributed service that can handle {@code writes}, each
 * of which transitions the database from one consistent state to another.
 * 
 * @author jnelson
 */
public interface WriteService {

	/**
	 * Add {@code column} as {@code value} for {@code row}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return {@code true} if {@code value} is added to the cell at {@code row}
	 *         x {@code column} where it did not previously exist
	 */
	public boolean add(String column, Object value, long row);

	/**
	 * Remove {@code column} as {@code value} for {@code row}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return {@code true} if {@code value} is removed from the cell at
	 *         {@code row} x {@code column} where it did previously exist
	 */
	public boolean remove(String column, Object value, long row);

}
