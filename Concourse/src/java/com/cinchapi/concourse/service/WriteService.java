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
package com.cinchapi.concourse.service;

import javax.annotation.Nullable;

/**
 * Specifies the possible writes that can occur regarding a {@code value} in
 * a {@code cell} located at the intersection of {@code row} and {@code column}.
 * 
 * @author jnelson
 */
public interface WriteService {
	
	/**
	 * Add {@code column} as {@code value} in a <em>new row</em>.
	 * 
	 * @param column
	 * @param value
	 * @return the {@code key} for the new row if the operation succeeds or
	 *         {@code null} if the operation fails
	 */
	@Nullable
	public long add(String column, Object value);

	/**
	 * Add {@code column} as {@code value} in {@code row}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if the cell at {@code row}:{@code column} contains
	 *         {@code value} after it previously did not.
	 */
	public boolean add(String column, Object value, long row);

	/**
	 * Remove {@code column} as {@code value} in {@code row}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if the cell at {@code row}:{@code column} no longer
	 *         contains {@code value} after it previously did.
	 */
	public boolean remove(String column, Object value, long row);

	/**
	 * Revert {@code column} to {@code timestamp} in {@code row}.
	 * 
	 * @param column
	 * @param timestamp
	 * @param row
	 * 
	 * @return {@code true} if the cell at {@code row}:{@code column} is
	 *         reverted.
	 */
	public boolean revert(String column, long timestamp, long row);

	/**
	 * Set {@code column} as {@code value} in {@code row} by removing any
	 * existing values and replacing them with {@code value}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return {@code true} if cell at {@code row}:{@code column} is modified to
	 *         contain only {@code value}.
	 */
	public boolean set(String column, Object value, long row);

}
