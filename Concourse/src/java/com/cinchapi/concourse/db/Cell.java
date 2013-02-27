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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.FixedSizeIterableByteSequences;
import com.cinchapi.concourse.util.IterableByteSequences;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Time;
import com.cinchapi.util.Hash;
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
 * <li>The size required for the <code>state</code> is equal to the sum of the
 * size of all the values contained within plus 4 additional bytes. A
 * <code>state</code> (and therefore a cell) can hold at most 44,739,242 values
 * at once, but in actuality this limit is much lower.</li>
 * <li>The size required for the <code>history</code> is 4 bytes plus the
 * summation of the size of each value multiplied by the number of times the
 * value was added plus that to the number of times the value was removed. A
 * history can hold at most 44,739,242 revisions, but in actuality this limit is
 * much lower.</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 */
public class Cell implements FileChannelPersistable, Locatable {

	/**
	 * Return a new cell instance.
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
	 * Return the cell represented by <code>bytes</code>. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * <code>bytes</code> was generated using {@link #getBytes()}.
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
		State state = State.fromByteSequences(s);

		byte[] h = new byte[historySize];
		bytes.get(h);
		History history = History.fromByteSequences(h);

		return new Cell(state, history, locator);
	}

	private static final int fixedSizeInBytes = 2 * (Integer.SIZE / 8); // stateSize
																		// and
																		// historySize
	private final byte[] locator;
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
	 * Add <code>value</code>, for which {@link Value#isForStorage()} must be
	 * <code>true</code>, to the cell. This will modify the <code>state</code>
	 * and log <code>value</code> in the <code>history</code>.
	 * 
	 * @param value
	 */
	public final void add(Value value) {
		Preconditions.checkState(state.canBeAdded(value),
				"Cannot add value '%s' because it is already contained", value);
		Preconditions.checkArgument(value.isForStorage(),
				"Cannot add value '%s' because it is not a forStorage value",
				value);
		state = state.add(value);
		history.log(value);
	}

	/**
	 * Return <code>true</code> if <code>value</code> is found in the cell.
	 * 
	 * @param value
	 * @return <code>true</code> if <code>value</code> is contained.
	 */
	public final boolean contains(Value value) {
		return state.contains(value);

	}

	/**
	 * Return the number of values contained in the cell.
	 * 
	 * @return the count.
	 */
	public int count() {
		return state.count();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Cell) {
			final Cell other = (Cell) obj;
			return Objects.equal(this.state, other.state)
					&& Objects.equal(this.history, other.history)
					&& Objects.equal(this.locator, other.locator);
		}
		return false;
	}

	@Override
	public byte[] getLocator() {
		return locator;
	}

	/**
	 * Return the values currently contained.
	 * 
	 * @return the values.
	 */
	public List<Value> getValues() {
		return state.getValues();
	}

	/**
	 * Return all the values contained <code>at</code> the specified timestamp.
	 * 
	 * @param at
	 * @return the values.
	 */
	public List<Value> getValues(long at) {
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
	public int hashCode() {
		return Objects.hashCode(state, history, locator);
	}

	/**
	 * Return <code>true</code> if the cell is empty.
	 * 
	 * @return <code>true</code> if the size is 0.
	 */
	public boolean isEmpty() {
		return state.size() == 0;
	}

	/**
	 * Remove <code>value</code>, for which {@link Value#isForStorage()} must be
	 * <code>true</code>, from the cell. This will modify the <code>state</code>
	 * and log <code>value</code> in the <code>history</code>.
	 * 
	 * @param value
	 */
	public void remove(Value value) {
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
	 * Iterate through and remove all of the <code>values</code> from the cell.
	 */
	public void removeAll() {
		Iterator<Value> it = getValues().iterator();
		while (it.hasNext()) {
			remove(it.next());
		}
	}

	@Override
	public String toString() {
		return "Cell " + Hash.toString(locator) + " with state "
				+ state.toString();
	}

	@Override
	public int size() {
		return state.size() + history.size() + locator.length
				+ fixedSizeInBytes;
	}

	@Override
	public byte[] getBytes() {
		return asByteBuffer().array();
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
	private ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putInt(state.size());
		buffer.putInt(history.size());
		buffer.put(locator);
		buffer.put(state.getBytes());
		buffer.put(history.getBytes());
		buffer.rewind();
		return buffer;
	}

	@Override
	public void writeTo(FileChannel channel) throws IOException {
		Writer.write(this, channel);

	}

	/**
	 * A log that appends a unique {@link Value} instance each time a
	 * modification is made to a {@link Cell} regarding the value.
	 * 
	 * @author jnelson
	 */
	private static final class History implements
			IterableByteSequences,
			FileChannelPersistable {

		/**
		 * Return the history represented by <code>bytes</code>. Use this method
		 * when reading and reconstructing from a file. This method assumes that
		 * <code>bytes</code> was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 * @return the history
		 */
		static History fromByteSequences(byte[] bytes) {
			return new History(bytes);
		}

		/**
		 * Return a history with <code>revisions</code>
		 * 
		 * @param revisions
		 * @return the history
		 */
		@SuppressWarnings("unused")
		static History fromList(List<Value> revisions) {
			return new History(revisions);
		}

		/**
		 * Return a new and empty history.
		 * 
		 * @return the new instance.
		 */
		static History newInstance() {
			return new History();
		}

		private List<Value> revisions;
		private final ByteBuffer bytes; // I am keeping the byte sequences here
										// instead of calculating them on demand
										// because I can't determine the size
										// needed store the state without
										// iterating through the list first
										// because each value may have a
										// different size.

		/**
		 * Construct a new instance using a byte buffer assuming that the
		 * <code>bytes</code> conform to the rules for a
		 * {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		private History(byte[] bytes) { // I prefer to receive a byte array
										// here so I know for sure one backs
										// the buffer
			this.bytes = ByteBuffer.wrap(bytes);
			this.revisions = Lists.newArrayList();
			IterableByteSequences.ByteSequencesIterator bsit = iterator();
			while (bsit.hasNext()) {
				revisions.add(Value.fromByteSequence((bsit.next())));
			}
		}

		/**
		 * Construct a new instance.
		 * 
		 * @param revisions
		 */
		private History(List<Value> revisions) {
			this.revisions = revisions;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			for (Value revision : revisions) {
				byte[] bytes = revision.getBytes();
				out.write(bytes.length);
				try {
					out.write(bytes);
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			this.bytes = ByteBuffer.wrap(out.toByteArray());
		}

		/**
		 * Construct a new instance. This constructor is for creating a
		 * new/empty history.
		 */
		private History() {
			this.revisions = Lists.newArrayList();
			this.bytes = ByteBuffer.allocate(0);
		}

		/**
		 * Count the total number of times that <code>value</code> appears in
		 * the history.
		 * 
		 * @param value
		 * @return the number of appearances.
		 */
		@SuppressWarnings("unused")
		public int count(Value value) {
			return count(value, Time.now());
		}

		/**
		 * Count the number of times that <code>value</code> appears in the
		 * history with a timestamp that is less than or equal to
		 * <code>at</code>.
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

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof History) {
				History other = (History) obj;
				return Objects.equal(revisions, other.revisions);
			}
			return false;
		}

		/**
		 * Return <code>true</code> if <code>value</code> existed in the cell
		 * prior <code>at</code> the specified timestamp (meaning there is an
		 * odd number of appearances for <code>value</code> in the history).
		 * 
		 * @param value
		 * @return <code>true</code> if <code>value</code> exists.
		 */
		@SuppressWarnings("unused")
		public boolean exists(Value value) {
			return exists(value, Time.now());
		}

		/**
		 * Return <code>true</code> if <code>value</code> existed in the cell
		 * prior <code>at</code> the specified timestamp (meaning there is an
		 * odd number of appearances for <code>value</code> in the history).
		 * 
		 * @param value
		 * @param before
		 * @return <code>true</code> if <code>value</code> existed.
		 */
		public boolean exists(Value value, long at) {
			return Numbers.isOdd(count(value, at));
		}

		/**
		 * Return a new list of revisions that were present <code>at</code> the
		 * specified timestamp.
		 * 
		 * @param to
		 * @return the list of revisions
		 */
		public List<Value> rewind(long to) {
			List<Value> snapshot = Lists.newArrayList();
			ListIterator<Value> it = revisions.listIterator();
			while (it.hasNext()) {
				Value value = it.next();
				if(value.getTimestamp() <= to) {
					snapshot.add(value);
				}
			}
			return snapshot;
		}

		@Override
		public byte[] getBytes() {
			return asByteBuffer().array();
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(revisions);
		}

		@Override
		public ByteSequencesIterator iterator() {
			return IterableByteSequences.ByteSequencesIterator.over(getBytes());
		}

		/**
		 * Log a revision for <code>value</code>
		 * 
		 * @param value
		 */
		public void log(Value value) {
			Preconditions.checkArgument(value.isForStorage());
			revisions.add(value);
		}

		@Override
		public int size() {
			return bytes.capacity();
		}

		@Override
		public void writeTo(FileChannel channel) throws IOException {
			Writer.write(this, channel);

		}

		/**
		 * Return a byte buffer that represents the object and conforms to
		 * {@link FixedSizeIterableByteSequences}.
		 * 
		 * @return a byte buffer
		 */
		private ByteBuffer asByteBuffer() {
			bytes.rewind();
			return bytes;
		}
	}

	/**
	 * An immutable representation of the current state for a {@link Cell}.
	 * 
	 * @author jnelson
	 */
	@Immutable
	private static final class State implements
			IterableByteSequences,
			FileChannelPersistable {

		/**
		 * Return the state represented by <code>bytes</code>. Use this method
		 * when reading and reconstructing from a file.
		 * 
		 * @param bytes
		 * @return the state
		 */
		static State fromByteSequences(byte[] bytes) {
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

		private final List<Value> values;
		private final ByteBuffer bytes; // I am keeping the byte sequences here
										// instead of calculating them on demand
										// because I can't determine the size
										// needed store the state without
										// iterating through the list first
										// because each value may have a
										// different size.

		/**
		 * Construct a new instance using a byte buffer assuming that the
		 * <code>bytes</code> conform to the rules for a
		 * {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		private State(byte[] bytes) {// I prefer to receive a byte array here so
										// I know for sure one backs the buffer
			this.bytes = ByteBuffer.wrap(bytes);
			this.values = Lists.newArrayList();
			IterableByteSequences.ByteSequencesIterator bsit = iterator();
			while (bsit.hasNext()) {
				values.add(Value.fromByteSequence(bsit.next()));
			}
		}

		/**
		 * Construct a new instance. This constructor is for duplicating cell
		 * states.
		 * 
		 * @param values
		 */
		private State(List<Value> values) {
			this.values = values;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			for (Value value : values) {
				byte[] bytes = value.getBytes();
				out.write(bytes.length);
				try {
					out.write(bytes);
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			this.bytes = ByteBuffer.wrap(out.toByteArray());
		}

		/**
		 * Construct a new instance. This constructor is for creating new/empty
		 * cells states.
		 */
		private State() {
			this.values = Lists.newArrayList();
			this.bytes = ByteBuffer.allocate(0);
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
		 * Return <code>true</code> if <code>value</code> is not contained in
		 * the state and can therefore be added.
		 * 
		 * @param value
		 * @return <code>true</code> if <code>value</code> can be added.
		 */
		public boolean canBeAdded(Value value) {
			return !contains(value);
		}

		/**
		 * Return <code>true</code> if <code>value</code> is contained in the
		 * state and can therefore be removed.
		 * 
		 * @param value
		 * @return <code>true</code> if <code>value</code> can be removed.
		 */
		public boolean canBeRemoved(Value value) {
			return contains(value);
		}

		/**
		 * Return <code>true</code> if the state contains <code>value</code>.
		 * 
		 * @param value
		 * @return <code>true</code> if the value is contained.
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

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof State) {
				final State other = (State) obj;
				return Objects.equal(values, other.getValues());
			}
			return false;
		}

		@Override
		public byte[] getBytes() {
			return asByteBuffer().array();
		}

		/**
		 * Return the values.
		 * 
		 * @return the values.
		 */
		public List<Value> getValues() {
			return Collections.unmodifiableList(values);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(values);
		}

		@Override
		public IterableByteSequences.ByteSequencesIterator iterator() {
			return IterableByteSequences.ByteSequencesIterator.over(bytes
					.array());
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

		@Override
		public int size() {
			return bytes.capacity();
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
			bytes.rewind();
			return bytes;
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
		 * intersection of <code>row</code> and <code>column</code>.
		 * 
		 * @param row
		 * @param column
		 * @return the applicable locator.
		 */
		private static byte[] calculateLocator(Key row, String column) {
			return Hash.sha256(row.getBytes(),
					column.getBytes(ByteBuffers.charset()));
		}
	}

}
