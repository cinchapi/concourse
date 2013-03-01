package com.cinchapi.concourse.model;

import java.util.Set;

import javax.annotation.Nullable;

import org.jetbrains.annotations.NotNull;

import com.google.common.primitives.UnsignedLong;

/**
 * <p>
 * A schema-less collection of rows where each is identified by a unique id and
 * contains a dynamic collection of columns, each of which can hold multiple
 * distinct values of any type.
 * </p>
 * <p>
 * At scale, this structure is a simple and effective store for big data that
 * contains many relationships, while also providing fast and familiar access to
 * normalized data. As a modified adjacency matrix, Concourse naturally
 * represents which nodes on a graph are connected to which other nodes: each
 * row and each column corresponds to a node and each value in the cell formed
 * at the intersection of the row and column corresponds to an edge between the
 * corresponding row node and column node on the graph--an edge whose weight is
 * equal to the value.
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
	public boolean add(@NotNull UnsignedLong row, @NotNull String column,
			@NotNull Object value);

	/**
	 * Return a list of columns in {@code row}.
	 * 
	 * @param row
	 * @return the set of <code>non-null<code> columns in {@code row}. A
	 *         null return value indicates that {@link #exists(UnsignedLong)}
	 *         for {@code row} is false.
	 */
	public Set<String> describe(@NotNull UnsignedLong row);

	/**
	 * Return {@code true} if {@code row} exists.
	 * 
	 * @param row
	 * @return {@code true} if {@link #describe(UnsignedLong)} for
	 *         {@code row} is not empty.
	 */
	public boolean exists(@NotNull UnsignedLong row);

	/**
	 * Return {@code true} if {@code column} exists in
	 * {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @return {@code true} if {@link #get(UnsignedLong, String)} for
	 *         {@code row} and </code>column</code> is not empty.
	 */
	public boolean exists(@NotNull UnsignedLong row, @NotNull String column);

	/**
	 * Return {@code true} if {@code row} contains {@code value}
	 * in {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is contained.
	 */
	public boolean exists(@NotNull UnsignedLong row, @NotNull String column,
			@NotNull Object value);

	/**
	 * Return the values in {@code column} for {@code row} sorted by
	 * timestamp.
	 * 
	 * @param row
	 * @param column
	 * @return the result set.
	 */
	public Set<Object> get(@NotNull UnsignedLong row, @NotNull String column);

	/**
	 * Remove {@code value} from {@code column} in {@code row}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return {@code true} if {@code value} is removed.
	 */
	public boolean remove(@NotNull UnsignedLong row, @NotNull String column,
			@NotNull Object value);

	/**
	 * Select the rows that satisify the {@code operator} in comparison to
	 * the appropriate number of {@code values}.
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the result set.
	 */
	public Set<UnsignedLong> select(@NotNull String column, Operator operator,
			@NotNull Object... values);

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
	public boolean set(@NotNull UnsignedLong row, @NotNull String column,
			@Nullable Object value);

	/**
	 * Return the number of existing rows.
	 * 
	 * @return the number of rows.
	 */
	public UnsignedLong size();

	/**
	 * The operators that can be used with
	 * {@link Concourse#select(String, Operator, Object...)}.
	 */
	public enum Operator {
		/**
		 * Select rows where at least one value in the column is a substring of
		 * the query.
		 */
		CONTAINS("CONTAINS"),
		/**
		 * Select rows where no value in the column is a substring of the query
		 */
		NOT_CONTAINS("NOT CONTAINS"),
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
		Operator(String sign) {
			this.sign = sign;
		}

		@Override
		public String toString() {
			return sign;
		}
	}

}
