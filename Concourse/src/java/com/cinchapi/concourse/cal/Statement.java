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
package com.cinchapi.concourse.cal;

import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * An object that represents the components of a Concourse Action Language (CAL)
 * statement.
 * 
 * @author jnelson
 */
@Immutable
public class Statement {

	/**
	 * Return a builder for a CALStatement.
	 * 
	 * @return the builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	private final Action action;
	private final List<Mapping> mappings;
	private final long row;
	private final String query;
	private final int limit;
	private final int skip;

	/**
	 * Construct a new instance.
	 * 
	 * @param action
	 * @param mappings
	 * @param row
	 * @param query
	 * @param limit
	 * @param skip
	 */
	private Statement(Action action, @Nullable List<Mapping> mappings,
			@Nullable long row, @Nullable String query, int limit, int skip) {
		this.action = action;
		this.mappings = mappings;
		this.row = row;
		this.query = query;
		this.limit = limit;
		this.skip = skip;
	}

	/**
	 * Return the action.
	 * 
	 * @return the action
	 */
	public Action getAction() {
		return action;
	}

	/**
	 * Return the limit.
	 * 
	 * @return the limit
	 */
	public int getLimit() {
		return limit;
	}

	/**
	 * Return the mappings in order.
	 * 
	 * @return the mappings
	 */
	public List<Mapping> getMappings() {
		return mappings;
	}

	/**
	 * Return the query.
	 * 
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * Return the row.
	 * 
	 * @return the row
	 */
	public long getRow() {
		return row;
	}

	/**
	 * Return the skip
	 * 
	 * @return the skip
	 */
	public int getSkip() {
		return skip;
	}

	@Override
	public String toString() {
		return com.cinchapi.common.Strings.toString(this);
	}

	public static final class Builder {

		private Action action = null;
		private String query = null;
		private long row = 0;
		private int limit = 0;
		private int skip = 0;
		private List<Mapping> mappings = Lists.newArrayList();

		private String currentColumn = null;
		private Object currentValue = null;

		/**
		 * Add a column to the CALStatement. Each call to this method should be
		 * followed by a call to {@link #addValue(Object)}.
		 * 
		 * @param column
		 * @return this
		 */
		public Builder addColumn(String column) {
			Preconditions
					.checkState(
							currentColumn == null,
							"Cannot add another column before first adding a value to the most recently added column.");
			currentColumn = column;
			return this;
		}

		/**
		 * Add a value to the CALStatement. Each call to this method should be
		 * preceeded by a call to {@link #addColumn(String)}.
		 * 
		 * @param value
		 * @return this
		 */
		public Builder addValue(Object value) {
			Preconditions.checkState(!Strings.isNullOrEmpty(currentColumn),
					"You must add a new column before adding a new operator");
			currentValue = value;
			mappings.add(new Mapping(currentColumn, currentValue));
			currentColumn = null;
			currentValue = null;
			return this;
		}

		/**
		 * Build the CALStatement
		 * 
		 * @return the CALStatement
		 */
		public Statement build() {
			return new Statement(action, mappings, row, query, limit, skip);
		}

		/**
		 * Set the {@link Action} for the CALStatement.
		 * 
		 * @param action
		 * @return this
		 */
		public Builder setAction(Action action) {
			Preconditions.checkState(this.action == null,
					"The action has already been set");
			this.action = action;
			return this;
		}

		/**
		 * Set the limit for a CALStatement SELECT query
		 * 
		 * @param limit
		 * @return this
		 */
		public Builder setLimit(int limit) {
			Preconditions.checkState(action == Action.SELECT,
					"A limit can only be set for a SELECT action");
			this.limit = limit;
			return this;
		}

		/**
		 * Set the query for a SELECT action CALStatement
		 * 
		 * @param query
		 * @return this
		 */
		public Builder setQuery(String query) {
			Preconditions.checkState(action == Action.SELECT,
					"A query can only be set for a SELECT action");
			Preconditions.checkState(this.query == null,
					"The query has already been set");
			this.query = query;
			return this;
		}

		/**
		 * Set the row that the CALstatement operates on.
		 * 
		 * @param row
		 * @return this
		 */
		public Builder setRow(long row) {
			Preconditions.checkState(action != Action.SELECT,
					"The SELECT action does not operate on a specific row");
			Preconditions.checkState(this.row == 0,
					"The row has already been set");
			this.row = row;
			return this;
		}

		/**
		 * Set the skip for a CALStatement SELECT query
		 * 
		 * @param skip
		 * @return this
		 */
		public Builder setSkip(int skip) {
			Preconditions.checkState(action == Action.SELECT,
					"A skip can only be set for a SELECT action");
			this.skip = skip;
			return this;
		}

		@Override
		public String toString() {
			return com.cinchapi.common.Strings.toString(this);
		}
	}

	/**
	 * A lightwight mapping from a column to a value used in a CALStatement.
	 * 
	 * @author jnelson
	 */
	public static final class Mapping {

		private final String column;
		private final Object value;

		public Mapping(String column, Object value) {
			this.column = column;
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Mapping) {
				Mapping other = (Mapping) obj;
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

	enum Action {
		ADD, REMOVE, SET, GET, SELECT, HELP
	}

}
