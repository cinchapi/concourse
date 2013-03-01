package com.cinchapi.concourse;

import java.util.Set;

/**
 * <p>
 * Concourse is a schemaless database that is designed for applications that
 * have large amounts of sparse data in read and write heavy environments.
 * Concourse comes with automatic indexing, data versioning and support for
 * transactions.
 * 
 * <h2>Intent</h2>
 * Concourse aims to be a service that is easy for developers to deploy, access
 * and scale with minimal tuning, while also being highly optimized for fast
 * read/write operations. With Concourse there is no need to declare any
 * structure up front--no schema, no tables, no keyspaces, no indexes, no column
 * families, etc. Instead, you simply write any value <sup>1</sup> to any column
 * in any row at any time and the necessary structure is created for you on
 * demand. <br>
 * <br>
 * <sup>1</sup> - You cannot write a duplicate value to a cell.
 * <h2>Data Model</h2>
 * Concourse is a big matrix where each row is a single, canonical object record
 * and each column is an attribute in the data universe. The intersection of a
 * row and column--a cell--specifies the <strong>values</strong><sup>2</sup> for
 * the relevant attribute on the relevant object.
 * <ul>
 * <li>Each value is versioned by timestamp.<sup>3</sup></li>
 * <li>Each cell sorts its values by timestamp in descending order and also
 * maintains a historical log of revisions.</li>
 * <li>An index of rows sorted by id, an index of columns sorted logically, and
 * a full text index of values are all maintained for optimal reads.</li>
 * </ul>
 * <sup>2</sup> - A cell can simultaneously hold many distinct values and
 * multiple types.<br>
 * <sup>3</sup> - Each value is guaranteed to have a unique timestamp.
 * 
 * <h2>Graph Model</h2>
 * As a matrix, Concourse naturally represents which nodes on a graph are
 * connected to which other nodes: each row and each column corresponds to a
 * node and each value in the cell formed at the intersection of the row and
 * column corresponds to an edge between the corresponding row node and column
 * node on the graph--an edge whose weight is equal to the value.
 * </p>
 * 
 * @author jnelson
 */
public interface Concourse {

	/**
	 * Add {@code value} to {@code column} in {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if the {@code value} is added.
	 */
	public boolean add(long row, String column, Object value);

	/**
	 * Return a list of columns in {@code row}.
	 * 
	 * @param row
	 * @return the set of <code>non-null<code> columns in {@code row}. A
	 *         null return value indicates that {@link #exists(long)} for
	 *         {@code row} is false.
	 */
	public Set<String> describe(long row);

	/**
	 * Return {@code true} if {@code row} exists.
	 * 
	 * @param row
	 * @return {@code true} if {@link #describe(long)} for {@code row}
	 *         is not empty.
	 */
	public boolean exists(long row);

	/**
	 * Return {@code true} if {@code column} exists in
	 * {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @return {@code true} if {@link #get(long, String)} for
	 *         {@code row} and </code>column</code> is not empty.
	 */
	public boolean exists(long row, String column);

	/**
	 * Return {@code true} if {@code row} contains {@code value}
	 * in {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is contained.
	 */
	public boolean exists(long row, String column, Object value);

	/**
	 * Return the values in {@code column} for {@code row} sorted by
	 * timestamp.
	 * 
	 * @param row
	 * @param column
	 * @return the result set.
	 */
	public Set<Object> get(long row, String column);

	/**
	 * Remove {@code value} from {@code column} in {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is removed.
	 */
	public boolean remove(long row, String column, Object value);

	/**
	 * Select the rows that satisify the {@code operator} in comparison to
	 * the appropriate number of {@code values}.
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the result set.
	 */
	public Set<Long> select(String column, SelectOperator operator,
			Object... values);

	/**
	 * Overwrite {@code column} in {@code row} with {@code value}
	 * . If {@code value} is {@code null} then {@code column} is
	 * deleted from {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is set.
	 */
	public boolean set(long row, String column, Object value);

	/**
	 * The operators that can be used with
	 * {@link Concourse#select(String, SelectOperator, Object...)}.
	 */
	public enum SelectOperator {
		/**
		 * Select rows where at least one value in the column is a substring of
		 * the query.
		 */
		CONTAINS("CONTAINS"),
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
		BETWEEN("<>");

		private String sign;

		/**
		 * Set the enum properties
		 * 
		 * @param sign
		 */
		SelectOperator(String sign) {
			this.sign = sign;
		}

		@Override
		public String toString() {
			return sign;
		}
	}

}
