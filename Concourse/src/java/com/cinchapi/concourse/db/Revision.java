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
package com.cinchapi.concourse.db;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * 
 * 
 * @author jnelson
 */
@Immutable
public class Revision implements ByteSized {

	/**
	 * Return a new revision.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @param timestamp
	 * @return the revision.
	 */
	public static Revision create(Key row, String column, Value value) {
		return new Revision(row, column, value);
	}
	
	private static final int fixedSizeInBytes = 0;

	private final Key row;
	private final String column;
	private final Value value;
	private final int size;

	/**
	 * Construct a new instance.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @param timestamp
	 */
	private Revision(Key row, String column, Value value) {
		Preconditions.checkNotNull(row);
		Preconditions.checkArgument(!column.contains(" "),
				"'%s' is an invalid column name because it contains spaces",
				column);
		Preconditions.checkArgument(Strings.isNullOrEmpty(column),
				"column name cannot be empty");
		Preconditions.checkNotNull(value);

		this.row = row;
		this.column = column;
		this.value = value;
	}

	/**
	 * @return the row
	 */
	public Key getRow() {
		return row;
	}

	/**
	 * @return the column
	 */
	public String getColumn() {
		return column;
	}

	/**
	 * @return the value
	 */
	public Value getValue() {
		return value;
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.ByteSized#size()
	 */
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}
}
