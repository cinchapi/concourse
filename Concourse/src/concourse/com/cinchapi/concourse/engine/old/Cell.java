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
package com.cinchapi.concourse.engine.old;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.cinchapi.common.Strings;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.io.ByteSized;
import com.cinchapi.concourse.io.ByteSizedCollections;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A {@link Value} collection contained at the intersection of a {@link Row} and
 * {@link Column}.
 * </p>
 * <p>
 * Each cell has two variable length components:
 * <ul>
 * <li><strong>State</strong> - An immutable snapshot of the forStorage values
 * currently in the cell. The state for the cell changes whenever a write occurs
 * to the cell.</li>
 * <li><strong>History</strong> - A list of forStorage values sorted by
 * timestamp in insertion/ascending order. Every time a write occurs, the
 * written value is logged in the history. From the perspective of the history,
 * a value V is considered present at time T if there are an odd number of
 * values in the history equal to V with a timestamp less than or equal to T.</li>
 * </ul>
 * </p>
 * <p>
 * <h2>Storage Requirements</h2>
 * <ul>
 * <li>The size required for the {@code state} is the summation of the size of
 * each values plus 4 bytes. A {@code state} (and therefore a cell) can
 * theoretically hold up to {@value #MAX_NUM_VALUES} values at once, but in
 * actuality this limit is much lower.</li>
 * <li>The size required for the {@code history} is the product of 4 times the
 * total number of revisions plus the summation of the size of each value
 * multiplied by the number revisions with that value. A history can
 * theoretically hold up to {@value #MAX_NUM_REVISIONS} revisions, but in
 * actuality this limit is much lower.</li>
 * </ul>
 * <strong>Note:</strong> The size of the Cell is guaranteed to increase by at
 * least the size of V for each revision involving V. Therefore, removing a
 * value from a Cell will not reduce the size of the cell, but will
 * <em>increase</em> it!
 * </p>
 * 
 * @author jnelson
 */
class Cell implements ByteSized {
	// NOTE: This class does not define hashCode() or equals() because the
	// defaults are the desired behaviour.

	/**
	 * Return the cell represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the cell
	 */
	static Cell fromByteSequence(ByteBuffer bytes) {
		int columnSize = bytes.getInt();
		int stateSize = bytes.getInt();
		int historySize = bytes.getInt();

		byte[] column = new byte[columnSize];
		bytes.get(column);

		byte[] stateBytes = new byte[stateSize];
		bytes.get(stateBytes);
		State state = State.fromByteSequences(ByteBuffer.wrap(stateBytes));

		byte[] historyBytes = new byte[historySize];
		bytes.get(historyBytes);
		History history = History.fromByteSequences(ByteBuffer
				.wrap(historyBytes));

		return new Cell(column, state, history);
	}

	/**
	 * Return a <em>new</em> cell for storage under {@code column}, with a clean
	 * state and no history.
	 * 
	 * @return the new instance
	 */
	static Cell newInstance(String column) {
		State state = State.newInstance();
		History history = History.newInstance();
		return new Cell(column.getBytes(ByteBuffers.charset()), state, history);
	}

	private static final int FIXED_SIZE_IN_BYTES = 3 * (Integer.SIZE / 8); // columnSize,
																			// stateSize,
																			// historySize

	/**
	 * The theoretical max number of values that can be simultaneously contained
	 * in the cell. In actuality this limit is much lower because the size of a
	 * value may vary widely.
	 */
	public static final int MAX_NUM_VALUES = State.MAX_NUM_VALUES;

	/**
	 * The theoretical max number of revisions that can occur to a cell. In
	 * actuality this limit is much lower because the size of a value may vary
	 * widely.
	 */
	public static final int MAX_NUM_REVISIONS = History.MAX_NUM_VALUES;

	/**
	 * The minimum size of a cell (e.g. the size of an empty Cell with no values
	 * or history).
	 */
	public static final int MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES;

	/**
	 * This is a more realistic measure of the storage required for a single
	 * cell with one revision.
	 */
	public static final int WEIGHTED_SIZE_IN_BYTES = MIN_SIZE_IN_BYTES
			+ (2 * Value.WEIGHTED_SIZE_IN_BYTES);

	/**
	 * The maximum allowable size of a cell.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	private final byte[] column;
	private State state;
	private History history;

	/**
	 * Construct a new instance.
	 * 
	 * @param column
	 * @param state
	 * @param history
	 */
	private Cell(byte[] column, State state, History history) {
		this.column = column;
		this.state = state;
		this.history = history;
	}

	@Override
	public byte[] getBytes() {
		return asByteBuffer().array();
	}

	@Override
	public int size() {
		return column.length + state.size() + history.size()
				+ FIXED_SIZE_IN_BYTES;
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	/**
	 * Add {@code value}, for which {@link Value#isForStorage()} must be
	 * {@code true}, to the cell. This will modify the {@code state} and log
	 * {@code value} in the {@code history}.
	 * 
	 * @param value
	 */
	void add(Value value) {
		Preconditions.checkState(state.isAddable(value),
				"Cannot add value '%s' because it is already contained", value);
		Preconditions.checkArgument(value.isForStorage(),
				"Cannot add value '%s' because it is not a forStorage value",
				value);
		state = state.add(value);
		history.log(value);
	}

	/**
	 * Return {@code true} if {@code value} is found in the current state.
	 * 
	 * @param value
	 * @return {@code true} if {@code value} is presently contained
	 */
	boolean contains(Value value) {
		return state.contains(value);
	}

	/**
	 * Return the number of values presently contained in the cell.
	 * 
	 * @return the count
	 */
	int count() {
		return state.count();
	}

	/**
	 * Return a string representation of the {@code column} name.
	 * 
	 * @return the column name
	 */
	String getColumn() {
		return new String(column, ByteBuffers.charset());
	}

	/**
	 * Return a list of the presently contained values.
	 * 
	 * @return the values
	 */
	List<Value> getValues() {
		return state.getValues();
	}

	/**
	 * Return a list of the values contained {@code at} the specified timestamp.
	 * 
	 * @param at
	 * @return the values
	 */
	List<Value> getValues(long at) {
		Iterator<Value> it = history.rewind(at).iterator();
		List<Value> snapshot = Lists.newArrayList();
		while (it.hasNext()) {
			Value value = it.next();
			if(snapshot.contains(value)) {
				snapshot.remove(value);
			}
			else {
				snapshot.add(value);
			}
		}
		return snapshot;
	}

	/**
	 * Return {@code true} if the cell is empty.
	 * 
	 * @return {@code true} if the state size is 0
	 */
	boolean isEmpty() {
		return state.size() == 0;
	}

	/**
	 * Remove {@code value}, for which {@link Value#isForStorage()} must be
	 * {@code true}, from the cell. This will modify the {@code state} and log
	 * {@code value} in the {@code history}.
	 * 
	 * @param value
	 */
	void remove(Value value) {
		Preconditions.checkState(state.isRemovable(value),
				"Cannot remove value '%s' because it is not contained", value);
		Preconditions
				.checkArgument(
						value.isForStorage(),
						"Cannot remove value '%s' because it is not a forStorage value",
						value);
		state = state.remove(value);
		history.log(Value.forStorage(value.getQuantity())); // Make a copy of
															// the value so it
															// has a distinct
															// timestamp in
															// history
	}

	/**
	 * Return a new byte buffer that contains the current view of the cell with
	 * the following order:
	 * <ol>
	 * <li><strong>columnSize</strong> - first 4 bytes</li>
	 * <li><strong>stateSize</strong> - next 4 bytes</li>
	 * <li><strong>historySize</strong> - next 4 bytes</li>
	 * <li><strong>column</strong> - next columnSize bytes</li>
	 * <li><strong>state</strong> - next stateSize bytes</li>
	 * <li><strong>history</strong> - remaining bytes</li>
	 * </ol>
	 * 
	 * @return a byte buffer.
	 */
	private ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putInt(column.length);
		buffer.putInt(state.size());
		buffer.putInt(history.size());
		buffer.put(column);
		buffer.put(state.getBytes());
		buffer.put(history.getBytes());
		buffer.rewind();
		return buffer;
	}

	/**
	 * The base class for the {@link Cell} elements that hold a List of values.
	 * The list is limited to a size of {@value #MAX_SIZE_IN_BYTES} bytes.
	 * 
	 * @author jnelson
	 */
	private static class AbstractValueList implements
			IterableByteSequences,
			ByteSized {

		protected static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;
		protected static final int MAX_NUM_VALUES = MAX_SIZE_IN_BYTES
				/ (4 + Value.MIN_SIZE_IN_BYTES);
		protected List<Value> values;

		/**
		 * Construct a new instance. This constructor is for creating a
		 * new/empty instance.
		 */
		private AbstractValueList() {
			this.values = Lists.newArrayList();
		}

		/**
		 * Construct a new instance using a byte buffer that conform to the
		 * rules for a {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		private AbstractValueList(ByteBuffer bytes) {
			this.values = Lists.newArrayList();
			byte[] array = new byte[bytes.remaining()];
			bytes.get(array);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(array);
			while (bsit.hasNext()) {
				values.add(Value.fromByteSequence((bsit.next())));
			}
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param values
		 */
		private AbstractValueList(List<Value> values) {
			this.values = values;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj.getClass().equals(this.getClass())) {
				final AbstractValueList other = (AbstractValueList) obj;
				return Objects.equal(values, other.values);
			}
			return false;
		}

		@Override
		public byte[] getBytes() {
			return ByteSizedCollections.toByteArray(values);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(values);
		}

		@Override
		public ByteSequencesIterator iterator() {
			return IterableByteSequences.ByteSequencesIterator.over(getBytes());
		}

		@Override
		public int size() {
			return getBytes().length;
		}

		@Override
		public String toString() {
			return values.toString();
		}
	}

	/**
	 * A log that appends a unique {@link Value} instance each time a
	 * write is made to the {@link Cell} for the value.
	 * 
	 * @author jnelson
	 */
	private static final class History extends AbstractValueList {

		/**
		 * Return the history represented by {@code bytes}. Use this method when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 * @return the history
		 */
		static History fromByteSequences(ByteBuffer bytes) {
			return new History(bytes);
		}

		/**
		 * Return a new and empty history.
		 * 
		 * @return the new instance.
		 */
		static History newInstance() {
			return new History();
		}

		/**
		 * Construct a new instance. This constructor is for creating a
		 * new/empty history.
		 */
		private History() {
			super();
		}

		/**
		 * Construct a new instance using a byte buffer assuming that the
		 * {@code bytes} conform to the rules for a
		 * {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		private History(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param revisions
		 */
		private History(List<Value> revisions) {
			super(revisions);
		}

		/**
		 * Count the total number of times that {@code value} appears in the
		 * history.
		 * 
		 * @param value
		 * @return the number of appearances
		 */
		// this method might be useful later
		@SuppressWarnings("unused")
		int count(Value value) {
			return count(value, Time.now());
		}

		/**
		 * Count the number of times that {@code value} appears in the history
		 * with a timestamp that is less than or equal to {@code at}.
		 * 
		 * @param value
		 * @param at
		 * @return the number of appearances
		 */
		int count(Value value, long at) {
			int count = 0;
			ListIterator<Value> it = rewind(at).listIterator();
			while (it.hasNext()) {
				count += it.next().equals(value) ? 1 : 0;
			}
			return count;
		}

		/**
		 * Return {@code true} if {@code value} presently exists in the cell.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} exists
		 */
		// this method might be useful later
		@SuppressWarnings("unused")
		boolean exists(Value value) {
			return exists(value, Time.now());
		}

		/**
		 * Return {@code true} if {@code value} existed in the cell prior
		 * to the specified timestamp (meaning there is an odd number of
		 * appearances for {@code value} in the history).
		 * 
		 * @param value
		 * @param before
		 * @return {@code true} if {@code value} existed.
		 */
		boolean exists(Value value, long at) {
			return Numbers.isOdd(count(value, at));
		}

		/**
		 * Log a revision for {@code value}
		 * 
		 * @param value
		 */
		void log(Value value) {
			Preconditions.checkArgument(value.isForStorage());
			values.add(value);
		}

		/**
		 * Return a <em>new</em> list of revisions that were present {@code at}
		 * the specified timestamp in insertion/ascending order.
		 * 
		 * @param to
		 * @return the list of revisions
		 */
		List<Value> rewind(long to) {
			List<Value> snapshot = Lists.newArrayList();
			Iterator<Value> it = values.iterator();
			while (it.hasNext()) {
				Value value = it.next();
				if(value.getTimestamp() <= to) {
					snapshot.add(value);
				}
				else {
					break; // since the values are sorted in insertion order, I
							// can stop looking once I find a timestamp greater
							// than {@code to}
				}
			}
			return snapshot;
		}

	}

	/**
	 * An immutable representation of the current state for a {@link Cell}.
	 * 
	 * @author jnelson
	 */
	@Immutable
	private static final class State extends AbstractValueList {

		/**
		 * Return the state represented by {@code bytes}. Use this method when
		 * reading and reconstructing from a file.
		 * 
		 * @param bytes
		 * @return the state
		 */
		static State fromByteSequences(ByteBuffer bytes) {
			return new State(bytes);
		}

		/**
		 * Create a state from a list of values.
		 * 
		 * @param values
		 * @return the state
		 */
		static State fromList(List<Value> values) {
			return new State(values);
		}

		/**
		 * Create a new, empty state.
		 * 
		 * @return the state
		 */
		static State newInstance() {
			return new State();
		}

		/**
		 * Construct a new instance. This constructor is for creating new/empty
		 * cells states.
		 */
		private State() {
			super();
		}

		/**
		 * Construct a new instance using a byte buffer assuming that the
		 * {@code bytes} conform to the rules for a
		 * {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		private State(ByteBuffer bytes) {
			super(bytes);
		}

		/**
		 * Construct a new instance. This constructor is for duplicating cell
		 * states.
		 * 
		 * @param values
		 */
		private State(List<Value> values) {
			super(values);
		}

		/**
		 * Call this method when performing a {@link Cell#add(Value)} write to
		 * get the new state representation. This method assumes
		 * that the caller has already verified that
		 * <code>value<code> {@link #isAddable(Value)}.
		 * 
		 * @param value
		 * @return the new state representation
		 */
		State add(Value value) {
			List<Value> values = Lists.newArrayList(this.values);
			values.add(value);
			return State.fromList(values);
		}

		/**
		 * Return {@code true} if the state contains {@code value}
		 * 
		 * @param value
		 * @return {@code true} if the value is contained
		 */
		boolean contains(Value value) {
			return values.contains(value);
		}

		/**
		 * Return the number of values in the state.
		 * 
		 * @return the count
		 */
		int count() {
			return values.size();
		}

		/**
		 * Return and unmodifiable view of the presently contained values.
		 * 
		 * @return the values
		 */
		List<Value> getValues() {
			return Collections.unmodifiableList(values);
		}

		/**
		 * Return {@code true} if {@code value} is not contained in the state
		 * and can therefore be added.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} can be added
		 */
		boolean isAddable(Value value) {
			return !contains(value);
		}

		/**
		 * Return {@code true} if {@code value} is contained in the state and
		 * can therefore be removed.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} can be removed
		 */
		boolean isRemovable(Value value) {
			return contains(value);
		}

		/**
		 * Call this method when performing a {@link Cell#remove(Value)} write
		 * to get the new state representation. This method assumes
		 * that the caller has already verified that
		 * <code>value<code> {@link #isRemovable(Value)}.
		 * 
		 * @param value
		 * @return the new state representation.
		 */
		State remove(Value value) {
			List<Value> values = Lists.newArrayList(this.values);
			values.remove(value);
			return State.fromList(values);
		}
	}
}
