package com.cinchapi.concourse.model;

import java.util.List;
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
 * {@link Concourse} can be used to represent which nodes on a graph are
 * connected to which other nodes. Each row and each column corresponds to a
 * node. Each value in the cell formed at the intersection of the row and column
 * corresponds to an edge between the corresponding row node and column node on
 * the graph--an edge whose weight is equal to the value.
 * </p>
 * 
 * @author jnelson
 */
public interface Concourse {

	/**
	 * Add <code>value</code> to <code>column</code> in <code>row</code>.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if the <code>value</code> is added.
	 */
	public boolean add(
			@NotNull UnsignedLong row,
			@NotNull String column,
			@NotNull Object value);

	/**
	 * Return a list of columns in <code>row</code>.
	 * 
	 * @param row
	 * @return the list of <code>non-null<code>columns in <code>row</code>.
	 */
	public List<String> describe(@NotNull UnsignedLong row);

	/**
	 * Return <code>true</code> if <code>row</code> exists.
	 * 
	 * @param row
	 * @return <code>true</code> if {@link #describe(UnsignedLong)} for
	 *         <code>row</code> is not empty.
	 */
	public boolean exists(@NotNull UnsignedLong row);
	
	/**
	 * Return <code>true</code> if <code>column</code> exists in
	 * <code>row</code>.
	 * 
	 * @param row
	 * @param column
	 * @return <code>true</code> if {@link #get(UnsignedLong, String)} for
	 *         <code>row</code> and </code>column</code> is not empty.
	 */
	public boolean exists(
			@NotNull UnsignedLong row,
			@NotNull String column);
	
	/**
	 * Return <code>true</code> if <code>row</code> contains <code>value</code>
	 * in <code>column</code>.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if <code>value</code> is contained.
	 */
	public boolean exists(
			@NotNull UnsignedLong row,
			@NotNull String column,
			@NotNull Object value);

	/**
	 * Return the values in <code>column</code> for <code>row</code> sorted by
	 * timestamp.
	 * 
	 * @param row
	 * @param column
	 * @return the result set.
	 */
	public Set<Object> get(
			@NotNull UnsignedLong row,
			@NotNull String column);

	/**
	 * Remove <code>value</code> from <code>column</code> in <code>row</code>.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if <code>value</code> is removed.
	 */
	public boolean remove(
			@NotNull UnsignedLong row,
			@NotNull String column,
			@NotNull Object value);

	/**
	 * Select the rows that satisify the <code>operator</code> in comparison to
	 * the appropriate number of <code>values</code>.
	 * 
	 * @param column
	 * @param operator
	 * @param values
	 * @return the result set.
	 */
	public Set<UnsignedLong> select(@NotNull String column, Operator operator,
			@NotNull Object...values);

	/**
	 * Overwrite <code>column</code> in <code>row</code> with <code>value</code>
	 * . If <code>value</code> is <code>null</code> then <code>column</code> is
	 * deleted from <code>row</code>.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return <code>true</code> if <code>value</code> is set.
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
		public String toString(){
			return sign;
		}
	}

}
