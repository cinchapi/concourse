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
package com.cinchapi.concourse.store.perm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.cinchapi.common.Hash;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.store.io.Locatable;
import com.cinchapi.concourse.store.io.Persistable;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * Represents a {@link Value} set contained at the intersection of a {@link Row}
 * and {@link Column}. Only a single cell can exist at this intersection, so
 * each is {@link Locatable} using a 32 byte hash derived from the row
 * {@link Key} and the Column Name. After construction, the cell does not
 * maintain any explicit knowledge of its housing row or column.
 * </p>
 * <p>
 * Each cell has two variable length components:
 * <ul>
 * <li><strong>State</strong> - An immutable snapshot of the forStorage values
 * currently in the cell. The state for the cell changes whenever a revision
 * occurs.</li>
 * <li><strong>History</strong> - A list of forStorage values sorted by
 * timestamp in ascending order. Every time a revision for a value occurs, that
 * value is logged in the history. From the perspective of the history, a value
 * V is considered present at time T if there are an odd number of values in the
 * history equal to V with a timestamp less than or equal to T.</li>
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
 * <strong>Note:</strong> The size of the Cell is guranteed to increase by at
 * least the size of V for each revision involving V. Therefore, removing a
 * value from a Cell will not reduce the size of the cell, but will
 * <em>increase</em> it!
 * </p>
 * <p>
 * <strong>Note:</strong> All the methods in this class are synchronized to
 * prevent state from changing in the middle of an operation
 * </p>
 * 
 * @author jnelson
 * @since 1.0
 */
public class Cell implements Persistable, Locatable {

	/**
	 * Return the cell represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the value
	 */
	public static Cell fromByteSequence(ByteBuffer bytes) {
		int stateSize = bytes.getInt();
		int historySize = bytes.getInt();

		byte[] locator = new byte[32];
		bytes.get(locator);

		byte[] s = new byte[stateSize];
		bytes.get(s);
		State state = State.fromByteSequences(ByteBuffer.wrap(s));

		byte[] h = new byte[historySize];
		bytes.get(h);
		History history = History.fromByteSequences(ByteBuffer.wrap(h));

		return new Cell(state, history, locator);
	}

	/**
	 * Return a new cell instance at the intersection of {@code row} and
	 * {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @return the cell
	 */
	public static Cell newInstance(Key row, String column) {
		State state = State.newInstance();
		History history = History.newInstance();
		byte[] locator = Utilities.calculateLocator(row, column);
		return new Cell(state, history, locator);
	}

	/**
	 * Return the {@code locator} for the cell at the intersection of
	 * {@code row} and {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @return the application cell locator.
	 */
	public static byte[] getLocatorFor(Key row, String column) {
		return Utilities.calculateLocator(row, column);
	}

	/**
	 * Read the next cell from {@code channel} assuming that it conforms to the
	 * specification described in the {@link #asByteBuffer()} method.
	 * 
	 * @param buffer
	 * @return the next {@link Cell} in the channel.
	 */
	public static Cell readFrom(FileChannel channel) throws IOException {
		ByteBuffer buffers[] = new ByteBuffer[3];
		buffers[0] = ByteBuffer.allocate(4); // stateSize
		buffers[1] = ByteBuffer.allocate(4); // historySize
		buffers[2] = ByteBuffer.allocate(32); // locator
		channel.read(buffers);

		for (ByteBuffer buf : buffers) {
			buf.rewind();
		}

		int stateSize = buffers[0].getInt();
		int historySize = buffers[1].getInt();
		byte[] locator = buffers[2].array();

		ByteBuffer s = ByteBuffer.allocate(stateSize);
		channel.read(s);
		s.rewind();
		State state = State.fromByteSequences(s);

		ByteBuffer h = ByteBuffer.allocate(historySize);
		channel.read(h);
		h.rewind();
		History history = History.fromByteSequences(h);

		return new Cell(state, history, locator);

	}

	private static final int FIXED_SIZE_IN_BYTES = 2 * (Integer.SIZE / 8) + 32; // stateSize,
																				// historySize,
																				// locator

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
	 * The maximum allowable size of a cell.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	private final byte[] locator; // SHA-256 hash (32 bytes)
	private State state;
	private History history;

	/**
	 * Construct a new instance.
	 * 
	 * @param state
	 * @param history
	 * @param locator
	 */
	private Cell(State state, History history, byte[] locator) {
		this.state = state;
		this.history = history;
		this.locator = locator;
	}

	/**
	 * Add {@code value}, for which {@link Value#isForStorage()} must be
	 * {@code true}, to the cell. This will modify the {@code state} and log
	 * {@code value} in the {@code history}.
	 * 
	 * @param value
	 */
	public synchronized final void add(Value value) {
		Preconditions.checkState(state.canBeAdded(value),
				"Cannot add value '%s' because it is already contained", value);
		Preconditions.checkArgument(value.isForStorage(),
				"Cannot add value '%s' because it is not a forStorage value",
				value);
		state = state.add(value);
		history.log(value);
	}

	/**
	 * Return {@code true} if {@code value} is found in the cell.
	 * 
	 * @param value
	 * @return {@code true} if {@code value} is contained.
	 */
	public synchronized final boolean contains(Value value) {
		return state.contains(value);

	}

	/**
	 * Return the number of values contained in the cell.
	 * 
	 * @return the count.
	 */
	public synchronized int count() {
		return state.count();
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if(obj instanceof Cell) {
			final Cell other = (Cell) obj;
			return Objects.equal(this.state, other.state)
					&& Objects.equal(this.history, other.history)
					&& Objects.equal(this.locator, other.locator);
		}
		return false;
	}

	@Override
	public synchronized byte[] getBytes() {
		return asByteBuffer().array();
	}

	/**
	 * Returns a 32 byte hash that represents the {@code row} and {@code column}
	 * that house the cell.
	 */
	@Override
	public byte[] getLocator() {
		return locator;
	}

	/**
	 * Return the values currently contained.
	 * 
	 * @return the values.
	 */
	public synchronized List<Value> getValues() {
		return state.getValues();
	}

	/**
	 * Return all the values contained {@code at} the specified timestamp.
	 * 
	 * @param at
	 * @return the values.
	 */
	public synchronized List<Value> getValues(long at) {
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

		return history.rewind(at);
	}

	@Override
	public synchronized int hashCode() {
		return Objects.hashCode(state, history, locator);
	}

	/**
	 * Return {@code true} if the cell is empty.
	 * 
	 * @return {@code true} if the size is 0.
	 */
	public synchronized boolean isEmpty() {
		return state.size() == 0;
	}

	/**
	 * Remove {@code value}, for which {@link Value#isForStorage()} must be
	 * {@code true}, from the cell. This will modify the {@code state} and log
	 * {@code value} in the {@code history}.
	 * 
	 * @param value
	 */
	public synchronized void remove(Value value) {
		Preconditions.checkState(state.canBeRemoved(value),
				"Cannot remove value '%s' because it is not contained", value);
		Preconditions
				.checkArgument(
						value.isForStorage(),
						"Cannot remove value '%s' because it is not a forStorage value",
						value);
		state = state.remove(value);
		history.log(value);
	}

	/**
	 * Iterate through and remove all of the {@code values} from the cell.
	 */
	public synchronized void removeAll() {
		Iterator<Value> it = getValues().iterator();
		while (it.hasNext()) {
			remove(it.next());
		}
	}

	@Override
	public synchronized int size() {
		return state.size() + history.size() + FIXED_SIZE_IN_BYTES;
	}

	@Override
	public synchronized String toString() {
		return "Cell " + Hash.toString(locator) + " with state "
				+ state.toString();
	}

	@Override
	public synchronized void writeTo(FileChannel channel) throws IOException {
		Writer.write(this, channel);
	}

	/**
	 * Return a new byte buffer that contains the current view of the cell with
	 * the following order:
	 * <ol>
	 * <li><strong>stateSize</strong> - first 4 bytes</li>
	 * <li><strong>historySize</strong> - next 4 bytes</li>
	 * <li><strong>locator</strong> - next 32 bytes</li>
	 * <li><strong>state</strong> - next stateSize bytes</li>
	 * <li><strong>history</strong> - remaining bytes</li>
	 * </ol>
	 * 
	 * @return a byte buffer.
	 */
	private synchronized ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putInt(state.size());
		buffer.putInt(history.size());
		buffer.put(locator);
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
			Persistable {

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
		 * Construct a new instance using a byte buffer assuming that the
		 * {@code bytes} conform to the rules for a
		 * {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		private AbstractValueList(ByteBuffer bytes) {
			this.values = Lists.newArrayList();
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(bytes.array());
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
			return asByteBuffer().array();
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
			return asByteBuffer().capacity();
		}

		@Override
		public String toString() {
			return values.toString();
		}

		@Override
		public void writeTo(FileChannel channel) throws IOException {
			Writer.write(this, channel);

		}

		/**
		 * Return a byte buffer that represents the object and conforms to
		 * {@link IterableByteSequences}.
		 * 
		 * @return a byte buffer
		 */
		private ByteBuffer asByteBuffer() {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			for (Value value : values) {
				byte[] bytes = value.getBytes();
				try {
					out.write(ByteBuffer.allocate(4).putInt(bytes.length)
							.array()); // for some reason writing the
										// length of the array doesn't work
										// properly, so I have to wrap it in a
										// byte buffer :-/
					out.write(bytes);
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			return ByteBuffer.wrap(out.toByteArray());
		}

	}

	/**
	 * A log that appends a unique {@link Value} instance each time a
	 * modification is made to a {@link Cell} regarding the value.
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
		 * @return the number of appearances.
		 */
		@SuppressWarnings("unused")
		public int count(Value value) { // this method might be useful later
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
		public int count(Value value, long at) {
			int count = 0;
			ListIterator<Value> it = rewind(at).listIterator();
			while (it.hasNext()) {
				count += it.next().equals(value) ? 1 : 0;
			}
			return count;
		}

		/**
		 * Return {@code true} if {@code value} existed in the cell prior
		 * {@code at} the specified timestamp (meaning there is an odd number of
		 * appearances for {@code value} in the history).
		 * 
		 * @param value
		 * @return {@code true} if {@code value} exists.
		 */
		@SuppressWarnings("unused")
		public boolean exists(Value value) { // this method might be useful
												// later
			return exists(value, Time.now());
		}

		/**
		 * Return {@code true} if {@code value} existed in the cell prior
		 * {@code at} the specified timestamp (meaning there is an odd number of
		 * appearances for {@code value} in the history).
		 * 
		 * @param value
		 * @param before
		 * @return {@code true} if {@code value} existed.
		 */
		public boolean exists(Value value, long at) {
			return Numbers.isOdd(count(value, at));
		}

		/**
		 * Log a revision for {@code value}
		 * 
		 * @param value
		 */
		public void log(Value value) {
			Preconditions.checkArgument(value.isForStorage());
			values.add(value);
		}

		/**
		 * Return a new list of revisions that were present {@code at} the
		 * specified timestamp.
		 * 
		 * @param to
		 * @return the list of revisions
		 */
		public List<Value> rewind(long to) {
			List<Value> snapshot = Lists.newArrayList();
			ListIterator<Value> it = values.listIterator();
			while (it.hasNext()) {
				Value value = it.next();
				if(value.getTimestamp() <= to) {
					snapshot.add(value);
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
		 * @return the state.
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
		 * Call this method when performing a {@link Cell#add(Value)}
		 * modification to get the new state representation. This method assumes
		 * that the caller has already verified that
		 * <code>value<code> {@link #canBeAdded(Value)}.
		 * 
		 * @param value
		 * @return the new state representation.
		 */
		public State add(Value value) {
			List<Value> values = Lists.newArrayList(this.values);
			values.add(value);
			return State.fromList(values);
		}

		/**
		 * Return {@code true} if {@code value} is not contained in the state
		 * and can therefore be added.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} can be added.
		 */
		public boolean canBeAdded(Value value) {
			return !contains(value);
		}

		/**
		 * Return {@code true} if {@code value} is contained in the state and
		 * can therefore be removed.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} can be removed.
		 */
		public boolean canBeRemoved(Value value) {
			return contains(value);
		}

		/**
		 * Return {@code true} if the state contains {@code value}.
		 * 
		 * @param value
		 * @return {@code true} if the value is contained.
		 */
		public boolean contains(Value value) {
			return values.contains(value);
		}

		/**
		 * Return the number of values in the state.
		 * 
		 * @return the count.
		 */
		public int count() {
			return values.size();
		}

		/**
		 * Return the values.
		 * 
		 * @return the values.
		 */
		public List<Value> getValues() {
			return Collections.unmodifiableList(values);
		}

		/**
		 * Call this method when performing a {@link Cell#remove(Value)}
		 * modification to get the new state representation. This method assumes
		 * that the caller has already verified that
		 * <code>value<code> {@link #canBeRemoved(Value)}.
		 * 
		 * @param value
		 * @return the new state representation.
		 */
		public State remove(Value value) {
			List<Value> values = Lists.newArrayList(this.values);
			values.remove(value);
			return State.fromList(values);
		}
	}

	/**
	 * {@link Cell} utilities.
	 * 
	 * @author jnelson
	 */
	private static class Utilities {

		/**
		 * Return the 32 byte locator value that corresponds to the cell at the
		 * intersection of {@code row} and {@code column}.
		 * 
		 * @param row
		 * @param column
		 * @return the applicable locator.
		 */
		private static byte[] calculateLocator(Key row, String column) {
			return Locators.create(row.getBytes(),
					column.getBytes(ByteBuffers.charset()));
		}
	}
}
