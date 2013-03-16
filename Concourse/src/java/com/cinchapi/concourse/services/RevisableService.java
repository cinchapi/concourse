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

/**
 * Specifies the possible revisions that can occur regarding a {@code value} in
 * a {@code cell} located at the intersection of {@code row} and {@code column}.
 * 
 * @author jnelson
 */
public interface RevisableService {

	/**
	 * Add {@code value} to cell at the intersection of {@code row} and
	 * {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 *            - any leading or trailing whitespace in a string value will be
	 *            stripped
	 * @return {@code true} if the {@code value} is added.
	 */
	public boolean add(long row, String column, Object value);

	/**
	 * Remove {@code value} from the cell at the intersection of {@code row} and
	 * {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is removed.
	 */
	public boolean remove(long row, String column, Object value);

	/**
	 * Revert the cell at the intersection of {@code row} and {@code column} to
	 * {@code timestamp}.
	 * 
	 * @param row
	 * @param column
	 * @param timestamp
	 * @return {@code true} if the cell is reverted
	 */
	public boolean revert(long row, String column, long timestamp);

	/**
	 * Remove any existing values from the cell at the intersection of
	 * {@code row} and {@code column} and replace them with {@code value}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is set.
	 */
	public boolean set(long row, String column, Object value);

}
