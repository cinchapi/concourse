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

/**
 * The operators that can be used with
 * {@link ReadService#query(String, Operator, Object...)}.
 * 
 * @author jnelson
 */
public enum Operator {
	/**
	 * Select rows where at least one value in the column is a substring of
	 * the query.
	 */
	SEARCH("SEARCH"),
	/**
	 * Select rows where at least one value in the column matches the regex
	 * query.
	 */
	REGEX("REGEX"),
	/**
	 * Select rows where no value in the column matches the regex query.
	 */
	NOT_REGEX("NOT REGEX"),
	/**
	 * Select rows where at least one value in the column is equal to the
	 * query.
	 */
	EQUALS("="),
	/**
	 * Select rows where no value in the column is equal to the query.
	 */
	NOT_EQUALS("!="),
	/**
	 * Select rows where at least one value in the column is greater than
	 * the query.
	 */
	GREATER_THAN(">"),
	/**
	 * Select rows where at least one value in the column is greater than or
	 * equal to the query.
	 */
	GREATER_THAN_OR_EQUALS(">="),
	/**
	 * Select rows where at least one value in the column is less than the
	 * query.
	 */
	LESS_THAN("<"),
	/**
	 * Select rows where at least one value in the column is less than or
	 * equal to the query.
	 */
	LESS_THAN_OR_EQUALS("<="),
	/**
	 * Select rows where at least one value in the column is between the
	 * queries.
	 */
	BETWEEN("BETWEEN");

	private String sign;

	/**
	 * Construct a new instance.
	 * 
	 * @param sign
	 */
	Operator(String sign) {
		this.sign = sign;
	}

	@Override
	public String toString() {
		return sign;
	}
}
