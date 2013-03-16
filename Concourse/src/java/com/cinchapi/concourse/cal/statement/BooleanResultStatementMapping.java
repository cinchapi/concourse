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
package com.cinchapi.concourse.cal.statement;

import com.google.common.base.Objects;

/**
 * A wrapping for {@code column} to {@code value} mappings specified in a
 * {@link BooleanResultStatement}.
 * 
 * @author jnelson
 */
public class BooleanResultStatementMapping {

	private final String column;
	private final Object value;

	public BooleanResultStatementMapping(String column, Object value) {
		this.column = column;
		this.value = value;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof BooleanResultStatementMapping) {
			BooleanResultStatementMapping other = (BooleanResultStatementMapping) obj;
			return Objects.equal(column, other.column)
					&& Objects.equal(value, other.value);
		}
		return false;
	}

	/**
	 * Return the {@code column}
	 * 
	 * @return the column
	 */
	public String getColumn() {
		return column;
	}

	/**
	 * Return the {@code value}
	 * 
	 * @return the value
	 */
	public Object getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(column, value);
	}

	@Override
	public String toString() {
		return com.cinchapi.common.Strings.toString(this);
	}

}
