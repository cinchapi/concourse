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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.io.ByteSized;
import com.cinchapi.concourse.io.ByteSizedCollections;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.*;

/**
 * <p>
 * This class represents a view of a cell, which is contained at the
 * intersection of a row and column. This view is identified by some {@code key}
 * <sup>1</sup> and maintains a collection of {@code objects} in insertion
 * order.
 * </p>
 * <p>
 * <sup>1</sup> - The identifier is used to locate they cell within a larger
 * collection (i.e. in a {@link Row} or {@link Column}).
 * </p>
 * <p>
 * In addition to its identifier, each cell has two variable length components:
 * <ul>
 * <li><strong>State</strong> - An immutable snapshot of the forStorage objects
 * currently in the cell. The state for the cell changes whenever a write occurs
 * to the cell.</li>
 * <li><strong>History</strong> - A list of forStorage objects sorted by
 * timestamp in insertion/ascending order. Every time a write occurs, the
 * written object is logged in the history. From the perspective of the history,
 * an object O is considered present at time T if there are an odd number of
 * objects in the history equal to O with a timestamp less than or equal to T.</li>
 * </ul>
 * </p>
 * <p>
 * <h2>Storage Requirements</h2>
 * <ul>
 * <li>The size required for the {@code state} is the summation of the size of
 * each object plus 4 bytes. A {@code state} (and therefore a cell) can
 * theoretically hold up to {@value #MAX_NUM_VALUES} values at once, but in
 * actuality this limit is much lower.</li>
 * <li>The size required for the {@code history} is the product of 4 times the
 * total number of revisions plus the summation of the size of each object
 * multiplied by the number revisions involving that object. A history can
 * theoretically hold up to {@value #MAX_NUM_REVISIONS} revisions, but in
 * actuality this limit is much lower.</li>
 * </ul>
 * <strong>Note:</strong> The size of the Cell is guaranteed to increase by at
 * least the size of O for each revision involving O. Therefore, removing an
 * item from a Cell will not reduce the size of the cell, but will
 * <em>increase</em> it!
 * </p>
 * 
 * @param <I> - the identifier type
 * @param <O> - the stored object type
 * @author jnelson
 */
public abstract class Cell<I extends ByteSized, O extends Storable> implements
		ByteSized {

	private static final int FIXED_SIZE_IN_BYTES = 3 * (Integer.SIZE / 8); // idSize,
																			// stateSize,
																			// historySize

	/**
	 * The maximum allowable size of a cell.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	/**
	 * The theoretical max number of values that can be simultaneously contained
	 * in the cell. In actuality this limit is much lower because the size of a
	 * value may vary widely.
	 */
	public static final int MAX_NUM_VALUES = MAX_SIZE_IN_BYTES
			/ (4 + Value.MIN_SIZE_IN_BYTES);

	/**
	 * The theoretical max number of revisions that can occur to a cell. In
	 * actuality this limit is much lower because the size of a value may vary
	 * widely.
	 */
	public static final int MAX_NUM_REVISIONS = MAX_NUM_VALUES;

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

	private final I id;
	private final State state;
	private final History history;

	/**
	 * Construct the cell instance represented by {@code bytes}. Use this
	 * constructor when reading and reconstructing from a file. This construct
	 * assumes that {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the cell
	 */
	protected Cell(ByteBuffer bytes) {
		int idSize = bytes.getInt();
		int stateSize = bytes.getInt();
		int historySize = bytes.getInt();

		byte[] idBytes = new byte[idSize];
		bytes.get(idBytes);
		this.id = getIdFromBytes(ByteBuffer.wrap(idBytes));

		byte[] stateBytes = new byte[stateSize];
		bytes.get(stateBytes);
		this.state = new State(ByteBuffer.wrap(stateBytes));

		byte[] historyBytes = new byte[historySize];
		bytes.get(historyBytes);
		this.history = new History(ByteBuffer.wrap(historyBytes));
	}

	/**
	 * Construct a <em>new</em> cell identified by {@code id}, with a clean
	 * state and no history.
	 * 
	 * @return the new instance
	 */
	protected Cell(I id) {
		this.id = id;
		this.state = new State();
		this.history = new History();
	}

	@Override
	public byte[] getBytes() {
		return asByteBuffer().array();
	}

	@Override
	public int size() {
		return id.size() + state.size() + history.size() + FIXED_SIZE_IN_BYTES;
	}

	/**
	 * Return the id that is represented by the {@code bytes}.
	 * 
	 * @param bytes
	 * @return the id
	 */
	protected abstract I getIdFromBytes(ByteBuffer bytes);

	/**
	 * Return the object that is represented by the {@code bytes}.
	 * 
	 * @param bytes
	 * @return the object
	 */
	protected abstract O getObjectFromBytes(ByteBuffer bytes);

	/**
	 * Add the forStorage{@code object} to the cell. This will modify the
	 * {@code state} and log {@code object} in the {@code history}.
	 * 
	 * @param object
	 */
	void add(O object) {
		// I am assuming that #object is a new forStorage instance, meaning it
		// has a unique timestamp.
		checkState(!exists(object),
				"Cannot add object '%s' because it is already contained",
				object);
		checkArgument(object.isForStorage(),
				"Cannot add object '%s' because it notForStorage", object);
		state.add(object);
		history.add(object);
	}

	/**
	 * Return the number of objects presently contained in the cell.
	 * 
	 * @return the count
	 */
	int count() {
		return state.count();
	}

	/**
	 * Return {@code true} if {@code object} is found in the current state.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is presently contained
	 */
	boolean exists(O object) {
		return state.contains(object);
	}

	/**
	 * Return {@code true} if {@code object} is found to exist in the history at
	 * {@code timestamp}.
	 * 
	 * @param object
	 * @param timestamp
	 * @return {@code true} if object was contained at {@code timestamp}
	 */
	boolean existsAt(O object, long timestamp) {
		return history.existsAt(object, timestamp);
	}

	/**
	 * Return a list of the presently contained objects.
	 * 
	 * @return the objects
	 */
	List<O> fetch() {
		return state.getObjects();
	}

	/**
	 * Return a list of the objects contained at {@code timestamp}.
	 * 
	 * @param timestamp
	 * @return the objects
	 */
	List<O> fetchAt(long timestamp) {
		Iterator<O> it = history.rewind(timestamp).iterator();
		List<O> snapshot = Lists.newArrayList();
		while (it.hasNext()) {
			O object = it.next();
			if(snapshot.contains(object)) {
				snapshot.remove(object);
			}
			else {
				snapshot.add(object);
			}
		}
		return snapshot;
	}

	/**
	 * Return the Cell identifier.
	 * 
	 * @return the id
	 */
	I getId() {
		return id;
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
	 * Remove the forStorage {@code object} from the cell. This will modify the
	 * {@code state} and log {@code object} in the {@code history}.
	 * 
	 * @param object
	 */
	void remove(O object) {
		// I am assuming that #object is a new forStorage instance, meaning it
		// has a unique timestamp.
		checkState(exists(object),
				"Cannot remove object '%s' because it is not contained", object);
		checkArgument(object.isForStorage(),
				"Cannot remove object '%s' because it is notForStorage", object);
		state.remove(object);
		history.remove(object);
	}

	/**
	 * Return a new byte buffer that contains the current view of the cell with
	 * the following order:
	 * <ol>
	 * <li><strong>idSize</strong> - first 4 bytes</li>
	 * <li><strong>stateSize</strong> - next 4 bytes</li>
	 * <li><strong>historySize</strong> - next 4 bytes</li>
	 * <li><strong>id</strong> - next idSize bytes</li>
	 * <li><strong>state</strong> - next stateSize bytes</li>
	 * <li><strong>history</strong> - remaining bytes</li>
	 * </ol>
	 * 
	 * @return a byte buffer.
	 */
	private ByteBuffer asByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocate(size());
		buffer.putInt(id.size());
		buffer.putInt(state.size());
		buffer.putInt(history.size());
		buffer.put(id.getBytes());
		buffer.put(state.getBytes());
		buffer.put(history.getBytes());
		buffer.rewind();
		return buffer;
	}

	/**
	 * A collection of {@code O} objects that are maintained in insertion order.
	 * Each object in the bucket must have a unique timestamp.
	 * 
	 * @author jnelson
	 */
	private abstract class Bucket implements IterableByteSequences, ByteSized {

		private final List<O> objects = Lists.newArrayList();

		/**
		 * The set of timestamps that appear in the history so that we can
		 * enforce the unique timestamp policy (all sorts of weird and undefined
		 * behaviour will happen if duplicate timestamps exist). This means that
		 * each Cell will take up an additional 16n bytes (n = number of
		 * revisions) in memory than on disk.
		 */
		private final HashSet<Long> timestamps = Sets.newHashSet();

		/**
		 * Construct a new instance.
		 */
		protected Bucket() { /* Do Nothing */}

		/**
		 * Construct a new instance using a byte buffer that conforms to the
		 * rules for a {@link IterableByteSequences}.
		 * 
		 * @param bytes
		 */
		protected Bucket(ByteBuffer bytes) {
			byte[] array = new byte[bytes.remaining()];
			bytes.get(array);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(array);
			while (bsit.hasNext()) {
				add(getObjectFromBytes(bsit.next()));
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if(obj.getClass().equals(this.getClass())) {
				final Bucket other = (Bucket) obj;
				return Objects.equal(objects, other.objects);
			}
			return false;
		}

		@Override
		public byte[] getBytes() {
			return ByteSizedCollections.toByteArray(objects);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(objects);
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
			return objects.toString();
		}

		/**
		 * Add {@code object} to the bucket.
		 * 
		 * @param object
		 * @return {@code true} if {@code object} is added
		 */
		boolean add(O object) {
			checkArgument(!timestamps.contains(object.getTimestamp()),
					"Cannot add %s because the associated timestamp "
							+ "already present in the bucket", object);
			return objects.add(object) && timestamps.add(object.getTimestamp());
		}

		/**
		 * Return {@code true} if {@code object} is present in the bucket
		 * according to the definition of {@link O#equals(Object)}.
		 * 
		 * @param object
		 * @return {@code true} if {@code object} is contained
		 */
		boolean contains(O object) {
			return objects.contains(object);
		}

		/**
		 * Return an unmodifiable view of the objects in the bucket.
		 * 
		 * @return the objects in the bucket
		 */
		List<O> getObjects() {
			return Collections.unmodifiableList(objects);
		}

		/**
		 * Remove the first instance of {@code object} from the bucket.
		 * 
		 * @param object
		 * @return {@code true} if the first instance of the {@code object} is
		 *         removed
		 */
		boolean remove(O object) {
			// I'm assuming that #object will have a different timestamp than
			// what has been stored so I must get the stored timestamp
			// and remove that instead of the timestamp of #object
			int index = objects.indexOf(object);
			if(index >= 0) {
				timestamps.remove(objects.remove(index).getTimestamp());
			}
			return false;
		}
	}

	/**
	 * An append-only log that keeps track of {@link Cell} writes over time. The
	 * {@code V} object associated with each write is associated with a
	 * timestamp and added to the end of the log. An object is considered to
	 * exist in the cell if it appears in the history an odd number of times,
	 * otherwise it is considered to not exist.
	 * 
	 * @author jnelson
	 */
	private final class History extends Bucket {

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
		 * Count the number of times that {@code object} appears in the history
		 * with a timestamp that is less than or equal to {@code at}.
		 * 
		 * @param object
		 * @param at
		 * @return the number of appearances
		 */
		int count(O object, long at) {
			int count = 0;
			ListIterator<O> it = rewind(at).listIterator();
			while (it.hasNext()) {
				count += it.next().equals(object) ? 1 : 0;
			}
			return count;
		}

		/**
		 * Return {@code true} if {@code object} existed in the cell prior
		 * to the specified timestamp (meaning there is an odd number of
		 * appearances for {@code object} in the history).
		 * 
		 * @param object
		 * @param before
		 * @return {@code true} if {@code object} existed.
		 */
		boolean existsAt(O object, long at) {
			return Numbers.isOdd(count(object, at));
		}

		@Override
		boolean remove(O object) {
			return add(object); // History is append only, so a removal means we
								// assume the object parameter has a new
								// timestamp and we add the object to the bucket
		}

		/**
		 * Return a <em>new</em> list of revisions that were present {@code at}
		 * the specified timestamp in insertion/ascending order.
		 * 
		 * @param timestamp
		 * @return the list of revisions
		 */
		List<O> rewind(long timestamp) {
			List<O> snapshot = Lists.newArrayList();
			Iterator<O> it = getObjects().iterator();
			while (it.hasNext()) {
				O object = it.next();
				if(object.getTimestamp() <= timestamp) {
					snapshot.add(object);
				}
				else {
					break; // since the objects are sorted in insertion order, I
							// can stop looking once I find a timestamp greater
							// than {@code to}
				}
			}
			return snapshot;
		}

	}

	/**
	 * A collection of the {@code V} objects that currently exist in the Cell.
	 * The current state can be derived from the {@link Cell#history}, but it is
	 * tracked explicitly for optimal performance.
	 * 
	 * @author jnelson
	 */
	private final class State extends Bucket {

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
		 * Return the number of objects in the state.
		 * 
		 * @return the count
		 */
		int count() {
			return getObjects().size();
		}
	}

}
