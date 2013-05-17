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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.mockito.Mockito;

import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.util.Strings;
import com.cinchapi.concourse.io.ByteSized;
import com.cinchapi.concourse.io.ByteSizedCollections;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.*;
import static org.mockito.Matchers.*;

/**
 * <p>
 * A {@code Container} is a version controlled series of {@link Containable}
 * values that is associated with a {@code key}. Containers are usually members
 * of a {@link Sequence}, but can be used in other contexts. The values in the
 * Container are maintained in insertion order and are represented by:
 * <ul>
 * <li><strong>State</strong> - A snapshot of the forStorage values currently in
 * the container. The state of the container changes whenever a relevant write
 * occurs.</li>
 * <li><strong>History</strong> - An append-only list of forStorage values
 * sorted by timestamp in insertion/ascending order. Every time a write occurs,
 * the written value is logged in the history. From the perspective of the
 * history, a value V is considered present at time T if there are an odd number
 * of values in the history equal to V with a timestamp less than or equal to T.
 * </li>
 * </ul>
 * <h2>Storage Requirements</h2>
 * <ul>
 * <li>The size required for the {@code state} is the summation of the size of
 * each value plus 4 bytes. A {@code state} (and therefore a container) can
 * theoretically hold up to {@value #MAX_NUM_VALUES} values at once, but in
 * actuality this limit is much lower.</li>
 * <li>The size required for the {@code history} is the product of 4 times the
 * total number of revisions plus the summation of the size of each value
 * multiplied by the number revisions involving that value. A history can
 * theoretically hold up to {@value #MAX_NUM_REVISIONS} revisions, but in
 * actuality this limit is much lower.</li>
 * </ul>
 * <strong>Note:</strong> Because a Container is version controlled, it's size
 * is guaranteed to increase by at least the size of V for each write.
 * </p>
 * 
 * @param <K> - the {@link ByteSized} key type
 * @param <V> - the {@link Containable} value type
 * @author jnelson
 */
abstract class Container<K extends ByteSized, V extends Containable> implements
		ByteSized {

	/**
	 * Return a <em>mock</em> container of {@code type}. Use this method instead
	 * of mocking {@code type} directly to ensure that the mock is compatible
	 * with the assumptions made in {@link Sequence}.
	 * 
	 * @param type
	 * @return the {@code container}
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T extends Container> T mock(Class<T> type) {
		T cell = Mockito.mock(type);
		Mockito.doNothing().when(cell).add(any(Containable.class));
		Mockito.doNothing().when(cell).remove(any(Containable.class));
		Mockito.doThrow(UnsupportedOperationException.class).when(cell)
				.getBytes();
		Mockito.doThrow(UnsupportedOperationException.class).when(cell).size();
		Mockito.doThrow(UnsupportedOperationException.class).when(cell)
				.getKeyFromByteSequence(any(ByteBuffer.class));
		Mockito.doThrow(UnsupportedOperationException.class).when(cell)
				.getValueFromByteSequence(any(ByteBuffer.class));
		return cell;
	}

	private static final int FIXED_SIZE_IN_BYTES = 3 * (Integer.SIZE / 8); // keySize,
																			// stateSize,
																			// historySize

	/**
	 * The maximum allowable size of a container.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	/**
	 * The theoretical max number of values that can be simultaneously
	 * contained in the container. In actuality this limit is much lower because
	 * the
	 * size of an value may vary widely.
	 */
	public static final int MAX_NUM_VALUES = MAX_SIZE_IN_BYTES
			/ (4 + Value.MIN_SIZE_IN_BYTES);

	/**
	 * The theoretical max number of revisions that can occur to a container. In
	 * actuality this limit is much lower because the size of an value may vary
	 * widely.
	 */
	public static final int MAX_NUM_REVISIONS = MAX_NUM_VALUES;

	/**
	 * The minimum size of a container (e.g. the size of an empty Container with
	 * no
	 * values or history).
	 */
	public static final int MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES;

	/**
	 * This is a more realistic measure of the storage required for a single
	 * container with one revision.
	 */
	public static final int WEIGHTED_SIZE_IN_BYTES = MIN_SIZE_IN_BYTES
			+ (2 * Value.WEIGHTED_SIZE_IN_BYTES);

	private final K key;
	private final State state;
	private final History history;

	/**
	 * Construct the container instance represented by {@code bytes}. Use this
	 * constructor when reading and reconstructing from a file. This constructor
	 * assumes that {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the Container
	 */
	protected Container(ByteBuffer bytes) {
		int idSize = bytes.getInt();
		int stateSize = bytes.getInt();
		int historySize = bytes.getInt();

		byte[] idBytes = new byte[idSize];
		bytes.get(idBytes);
		this.key = getKeyFromByteSequence(ByteBuffer.wrap(idBytes));

		byte[] stateBytes = new byte[stateSize];
		bytes.get(stateBytes);
		this.state = new State(ByteBuffer.wrap(stateBytes));

		byte[] historyBytes = new byte[historySize];
		bytes.get(historyBytes);
		this.history = new History(ByteBuffer.wrap(historyBytes));
	}

	/**
	 * Construct a <em>new</em> container identified by {@code id}, with a clean
	 * state and no history.
	 * 
	 * @return the new Container
	 */
	protected Container(K key) {
		this.key = key;
		this.state = new State();
		this.history = new History();
	}

	@Override
	public byte[] getBytes() {
		return asByteBuffer().array();
	}

	@Override
	public int size() {
		return key.size() + state.size() + history.size() + FIXED_SIZE_IN_BYTES;
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	/**
	 * Return the key that is represented by the sequence of {@code bytes}.
	 * 
	 * @param bytes
	 * @return the key
	 */
	protected abstract K getKeyFromByteSequence(ByteBuffer bytes);

	/**
	 * Return the value that is represented by the sequence of {@code bytes}.
	 * 
	 * @param bytes
	 * @return the value
	 */
	protected abstract V getValueFromByteSequence(ByteBuffer bytes);

	/**
	 * Add the forStorage {@code value} to the container. This will modify the
	 * {@code state} and log {@code value} in the {@code history}.
	 * 
	 * @param value
	 * @throws IllegalStateException if {@code value} is already contained
	 * @throws IllegalArgumentException if {@code value} is notForStorage
	 */
	void add(V value) throws IllegalStateException, IllegalArgumentException {
		// I am assuming that #value is a new forStorage instance, meaning it
		// has a unique timestamp. The caller must ensure compliance.
		checkState(!exists(value),
				"Cannot add value '%s' because it is already contained", value);
		checkArgument(value.isForStorage(),
				"Cannot add value '%s' because it notForStorage", value);
		state.add(value);
		history.add(value);
	}

	/**
	 * Return the number of values presently contained in the container.
	 * 
	 * @return the count
	 */
	int count() {
		return state.count();
	}

	/**
	 * Return {@code true} if {@code value} is found in the current
	 * {@code state}.
	 * 
	 * @param value
	 * @return {@code true} if {@code value} is presently contained
	 */
	boolean exists(V value) {
		return state.contains(value);
	}

	/**
	 * Return {@code true} if {@code value} is found to exist in the history at
	 * {@code timestamp}.
	 * 
	 * @param value
	 * @param timestamp
	 * @return {@code true} if value was contained at {@code timestamp}
	 */
	boolean exists(V value, long timestamp) {
		return history.existsAt(value, timestamp);
	}

	/**
	 * Return the key.
	 * 
	 * @return the id
	 */
	K getKey() {
		return key;
	}

	/**
	 * Return a list of the presently contained values.
	 * 
	 * @return the values
	 */
	List<V> getValues() {
		return state.getValues();
	}

	/**
	 * Return a list of the values contained at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the values
	 */
	List<V> getValues(long timestamp) {
		Iterator<V> it = history.rewind(timestamp).iterator();
		List<V> snapshot = Lists.newArrayList();
		while (it.hasNext()) {
			V value = it.next();
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
	 * Return {@code true} if the container is empty.
	 * 
	 * @return {@code true} if the state count is 0
	 */
	boolean isEmpty() {
		return state.count() == 0;
	}

	/**
	 * Remove the forStorage {@code value} from the container. This will modify
	 * the {@code state} and log {@code value} in the {@code history}.
	 * 
	 * @param value
	 * @throws IllegalStateException if {@code value} is not contained
	 * @throws IllegalArgumentException if {@code value} is notForStorage
	 */
	void remove(V value) throws IllegalStateException, IllegalArgumentException {
		// I am assuming that #value is a new forStorage instance, meaning it
		// has a unique timestamp. The caller must ensure compliance.
		checkState(exists(value),
				"Cannot remove value '%s' because it is not contained", value);
		checkArgument(value.isForStorage(),
				"Cannot remove value '%s' because it is notForStorage", value);
		state.remove(value);
		history.remove(value);
	}

	/**
	 * Return a new byte buffer that contains the current view of the container
	 * with
	 * the following order:
	 * <ol>
	 * <li><strong>keySize</strong> - first 4 bytes</li>
	 * <li><strong>stateSize</strong> - next 4 bytes</li>
	 * <li><strong>historySize</strong> - next 4 bytes</li>
	 * <li><strong>key</strong> - next idSize bytes</li>
	 * <li><strong>state</strong> - next stateSize bytes</li>
	 * <li><strong>history</strong> - remaining bytes</li>
	 * </ol>
	 * 
	 * @return a byte buffer.
	 */
	private ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putInt(key.size());
		buffer.putInt(state.size());
		buffer.putInt(history.size());
		buffer.put(key.getBytes());
		buffer.put(state.getBytes());
		buffer.put(history.getBytes());
		buffer.rewind();
		return buffer;
	}

	/**
	 * A collection of {@code V} values that are maintained in insertion order
	 * with a Container. Each value in the container must have a unique
	 * timestamp.
	 * 
	 * @author jnelson
	 */
	private abstract class Content implements IterableByteSequences, ByteSized {

		private final List<V> values = Lists.newArrayList();

		/**
		 * Construct a new instance.
		 */
		protected Content() { /* Do Nothing */}

		/**
		 * Construct a new instance using a byte buffer that conforms to the
		 * rules for {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		protected Content(ByteBuffer bytes) {
			byte[] array = new byte[bytes.remaining()];
			bytes.get(array);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(array);
			while (bsit.hasNext()) {
				add(getValueFromByteSequence(bsit.next()));
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if(obj.getClass().equals(this.getClass())) {
				final Content other = (Content) obj;
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

		/**
		 * Add {@code value} to the container.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} is added
		 * @throws IllegalArgumentException if the #timestamp associated with
		 *             {@code value} is not greater than the most recent
		 *             timestamp in the container
		 */
		boolean add(V value) throws IllegalArgumentException {
			// I check against the last timestamp to ensures that the list is
			// kept in insertion order and that no duplicate timestamps are
			// ever allowed
			long timestamp = values.isEmpty() ? Containable.NIL : values.get(
					values.size() - 1).getTimestamp();
			checkArgument(value.getTimestamp() > timestamp,
					"Cannot add %s because it's associated timestamp "
							+ "is less than the most recent timestamp "
							+ "in the container", value);
			return values.add(value);
		}

		/**
		 * Return {@code true} if {@code value} is present in the container
		 * according to the definition of {@link V#equals(Value)}.
		 * 
		 * @param value
		 * @return {@code true} if {@code value} is contained
		 */
		boolean contains(V value) {
			return values.contains(value);
		}

		/**
		 * Return an unmodifiable view of the values in the container.
		 * 
		 * @return the values in the container
		 */
		List<V> getValues() {
			return Collections.unmodifiableList(values);
		}

		/**
		 * Remove the first instance of {@code value} from the container.
		 * 
		 * @param value
		 * @return {@code true} if the first instance of the {@code value} is
		 *         removed
		 */
		boolean remove(V value) {
			return values.remove(value);
		}
	}

	/**
	 * An append-only log that keeps track of {@link Container} writes over
	 * time. The {@code V} value associated with each write is associated with a
	 * timestamp and added to the end of the log. An value is considered to
	 * exist in the container if it appears in the history an odd number of
	 * times,
	 * otherwise it is considered to not exist.
	 * 
	 * @author jnelson
	 */
	private final class History extends Content {

		/**
		 * Construct a new instance.
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
		 * Count the number of times that {@code value} appears in the history
		 * with a timestamp that is less than or equal to {@code at}.
		 * 
		 * @param value
		 * @param at
		 * @return the number of appearances
		 */
		int count(V value, long at) {
			int count = 0;
			ListIterator<V> it = rewind(at).listIterator();
			while (it.hasNext()) {
				count += it.next().equals(value) ? 1 : 0; // I'm assuming that
															// {@link
															// V#equals(Value)}
															// does not account
															// for #timestamp
			}
			return count;
		}

		/**
		 * Return {@code true} if {@code value} existed in the container prior
		 * to the specified timestamp (meaning there is an odd number of
		 * appearances for {@code value} in the history).
		 * 
		 * @param value
		 * @param before
		 * @return {@code true} if {@code value} existed.
		 */
		boolean existsAt(V value, long at) {
			return Numbers.isOdd(count(value, at));
		}

		@Override
		boolean remove(V value) {
			return add(value); // History is append only, so a removal means we
								// assume the #value has a new timestamp and is
								// added
		}

		/**
		 * Return a <em>new</em> list of revisions that were present {@code at}
		 * the specified timestamp in insertion/ascending order.
		 * 
		 * @param timestamp
		 * @return the list of revisions
		 */
		List<V> rewind(long timestamp) {
			List<V> snapshot = Lists.newArrayList();
			Iterator<V> it = getValues().iterator();
			while (it.hasNext()) {
				V value = it.next();
				if(value.getTimestamp() <= timestamp) {
					snapshot.add(value);
				}
				else {
					break; // since the values are stored in insertion order, I
							// can stop looking once I find a timestamp greater
							// than the parameter
				}
			}
			return snapshot;
		}

	}

	/**
	 * A collection of the {@code V} values that currently exist in the
	 * Container.
	 * The current state can be derived from the {@link Container#history}, but
	 * it is
	 * tracked explicitly for optimal performance.
	 * 
	 * @author jnelson
	 */
	private final class State extends Content {

		/**
		 * Construct a new instance.
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
		 * Return the number of values in the state.
		 * 
		 * @return the count
		 */
		int count() {
			return getValues().size();
		}
	}

}
