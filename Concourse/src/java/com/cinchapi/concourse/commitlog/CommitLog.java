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
package com.cinchapi.concourse.commitlog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.Strings;
import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.api.ConcourseService;
import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.db.Value;
import com.cinchapi.concourse.db.api.Persistable;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * <p>
 * A temporary data store. The CommitLog is memory mapped and append-only which
 * enables fast writes to occur. Despite being a temporary store, the CommitLog
 * is a fully functional {@link ConcourseService} that is simultaneously
 * maintained on disk and in memory. Occasionally, the CommitLog is flushed to a
 * permanent database. The size of the CommitLog on disk cannot exceed
 * {@value #MAX_SIZE_IN_BYTES} bytes, but it will take up up to 4X more space in
 * memory than it does on disk.
 * </p>
 * 
 * 
 * @author Jeff Nelson
 */
public class CommitLog extends ConcourseService implements
		IterableByteSequences,
		Persistable {

	/**
	 * Return the CommitLog represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param buffer
	 * @return the commitLog
	 */
	public static CommitLog fromByteSequences(MappedByteBuffer buffer) {
		byte[] bytes = new byte[buffer.capacity()];
		buffer.get(bytes);

		Memory memory = Memory.newInstancewithExpectedSize(bytes.length
				/ (Commit.AVG_MIN_SIZE_IN_BYTES + 4)); // I'm adding 4
														// to
														// account for
														// the 4
														// bytes used to
														// store
														// the size for
														// each
														// commit
		IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
				.over(bytes);
		while (bsit.hasNext()) {
			memory.add(Commit.fromByteSequence(bsit.next()));
		}

		return new CommitLog(buffer, memory);
	}

	/**
	 * Return the CommitLog that is stored in the file at {@code location}.
	 * 
	 * @param location
	 *            - path to the FILE (not directory) to use for the CommitLog
	 * @return the CommitLog stored at {@code location}
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static CommitLog fromFile(String location)
			throws FileNotFoundException, IOException {
		int size = (int) new File(location).length();
		Preconditions.checkArgument(size < MAX_SIZE_IN_BYTES,
				"The size of the CommitLog cannot be greater than {}",
				MAX_SIZE_IN_BYTES);
		MappedByteBuffer buffer = new RandomAccessFile(location, "rw")
				.getChannel().map(MapMode.READ_WRITE, 0, size);
		return CommitLog.fromByteSequences(buffer);
	}

	/**
	 * Return a new CommitLog instance at {@code location}. This commit log will
	 * overwrite anything that was previously written to the file at
	 * {@code location}.
	 * 
	 * @param location
	 *            - path to the FILE (not directory) to use for the CommitLog
	 * @param size
	 *            - a larger CommitLog optimizes write time whereas a smaller
	 *            CommitLog optimizes read time
	 * @return the new commit log
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static CommitLog newInstance(String location, int size)
			throws IOException {
		Preconditions.checkArgument(size < MAX_SIZE_IN_BYTES,
				"The size of the CommitLog cannot be greater than {}",
				MAX_SIZE_IN_BYTES);

		File tmp = new File(location);
		tmp.delete(); // delete the old file if it was there
		tmp.getParentFile().mkdirs();
		tmp.createNewFile();

		MappedByteBuffer buffer = new RandomAccessFile(location, "rw")
				.getChannel().map(MapMode.READ_WRITE, 0, size);
		Memory memory = Memory.newInstancewithExpectedSize(size
				/ Commit.AVG_MIN_SIZE_IN_BYTES);
		return new CommitLog(buffer, memory);
	}

	private static final int FIXED_SIZE_PER_COMMIT = Integer.SIZE / 8; // for
																		// storing
																		// the
																		// size
																		// of
																		// of
																		// the
																		// commit

	/**
	 * <p>
	 * The percent of capacity to reserve for overflow protection. This is a
	 * safeguard against cases when the commitlog has M bytes of capacity
	 * remaining and the next commit requires N bytes of storage (N > M).
	 * Technically the commitlog is not full, but it would experience an
	 * overflow if it tired to add the commit.
	 * </p>
	 * <p>
	 * Setting this value to X means that a commitlog with a total capacity of Z
	 * will be deemed full when it reaches a capacity of Y (Y = 100-X * Z). Any
	 * capacity < Y is not considered full whereas any capacity > Y is
	 * considered full. So when the commitlog is M bytes away from reaching a
	 * capacity of Y, adding a commit that requires N bytes (N > M) will not
	 * cause an overflow so long as N-M < Z-Y.
	 * </p>
	 */
	public static final double PCT_CAPACITY_FOR_OVERFLOW_PREVENTION = .02;

	/**
	 * The default location for the CommtLog file.
	 */
	public static final String DEFAULT_LOCATION = "db/commitlog";

	/**
	 * The maximum allowable size of the CommitLog on disk.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	/**
	 * The default size of the CommitLog.
	 */
	public static final int DEFAULT_SIZE_IN_BYTES = MAX_SIZE_IN_BYTES;

	private static final Logger log = LoggerFactory.getLogger(CommitLog.class);
	private static final double PCT_USABLE_CAPACITY = 100 - PCT_CAPACITY_FOR_OVERFLOW_PREVENTION;

	private final Memory memory;
	private final MappedByteBuffer buffer;
	private int size = 0;
	private final int usableCapacity;

	/**
	 * 
	 * Construct a new instance.
	 * 
	 * @param buffer
	 * @param memory
	 */
	private CommitLog(MappedByteBuffer buffer, Memory memory) {
		this.buffer = buffer;
		this.memory = memory;
		this.usableCapacity = (int) Math.round((PCT_USABLE_CAPACITY / 100.0)
				* buffer.capacity());
		this.buffer.force();
	}

	@Override
	public byte[] getBytes() {
		ByteBuffer copy = buffer.asReadOnlyBuffer();
		copy.rewind();
		byte[] bytes = new byte[size];
		copy.get(bytes);
		return bytes;
	}

	/**
	 * Return a list of the commits.
	 * 
	 * @return the commits
	 */
	public List<Commit> getCommits() {
		synchronized (memory) {
			return memory.getCommits();
		}
	}

	/**
	 * Return {@code true} if the commit log is full and should be
	 * {@code flushed}.
	 * 
	 * @return {@code true} if the commit log is full.
	 */
	public boolean isFull() {
		return size >= usableCapacity;
	}

	@Override
	public ByteSequencesIterator iterator() {
		synchronized (buffer) {
			byte[] array = new byte[size];
			buffer.rewind();
			buffer.get(array);
			return ByteSequencesIterator.over(array);
		}
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	@Override
	public void writeTo(FileChannel channel) throws IOException {
		Writer.write(this, channel);

	}

	@Override
	protected boolean addSpi(long row, String column, Object value) {
		if(!exists(row, column, value)) {
			return commit(Commit.forStorage(row, column, value));
		}
		return false;
	}

	/**
	 * Return the number of commits that exist for the revision for
	 * {@code value} in the {@code cell} at the intersection of {@code row} and
	 * {@code column}.
	 * 
	 * @param row
	 * @param column
	 * @param value
	 * @return the number of commits for the revision
	 */
	protected int count(long row, String column, Object value) {
		Commit commit = Commit.notForStorage(row, column, value);
		synchronized (memory) {
			return memory.count(commit);
		}
	}

	@Override
	protected Set<String> describeSpi(long row) {
		Map<String, Set<Value>> columns2Values = Maps.newHashMap();
		synchronized (memory) {
			Iterator<Commit> commiterator = memory.getCommits().iterator();
			while (commiterator.hasNext()) {
				Commit commit = commiterator.next();
				if(Longs.compare(commit.getRow().asLong(), row) == 0) {
					Set<Value> values;
					if(columns2Values.containsKey(commit.getColumn())) {
						values = columns2Values.get(commit.getColumn());
					}
					else {
						values = Sets.newHashSet();
						columns2Values.put(commit.getColumn(), values);
					}
					if(values.contains(commit.getValue())) { // this means I've
																// encountered
																// an
																// even number
																// commit for
																// row/column/value
																// which
																// resulted
																// from a
																// removal
						values.remove(commit.getValue());
					}
					else {
						values.add(commit.getValue());
					}
				}
			}
			Set<String> columns = columns2Values.keySet();
			Iterator<String> coliterator = columns.iterator();
			while (coliterator.hasNext()) {
				if(columns2Values.get(coliterator.next()).isEmpty()) {
					coliterator.remove();
				}
			}
			return columns;
		}
	}

	@Override
	protected boolean existsSpi(long row, String column, Object value) {
		return Numbers.isOdd(count(row, column, value));
	}

	@Override
	protected Set<Object> getSpi(long row, String column) {
		Set<Value> _values = Sets.newLinkedHashSet();
		ListIterator<Commit> commiterator = memory.getCommits().listIterator(
				memory.getCommits().size());
		while (commiterator.hasPrevious()) {
			Commit commit = commiterator.previous();
			if(Longs.compare(commit.getRow().asLong(), row) == 0
					&& commit.getColumn().equals(column)) {
				if(_values.contains(commit.getValue())) { // this means I've
															// encountered an
															// even number
															// commit for
															// row/column/value
															// which resulted
															// from a removal
					_values.remove(commit.getValue());
				}
				else {
					_values.add(commit.getValue());
				}
			}
		}
		Set<Object> values = Sets.newLinkedHashSetWithExpectedSize(_values
				.size());
		for (Value value : _values) {
			values.add(value.getQuantity());
		}
		return values;
	}

	@Override
	protected boolean removeSpi(long row, String column, Object value) {
		if(exists(row, column, value)) {
			return commit(Commit.forStorage(row, column, value));
		}
		return false;
	}

	@Override
	protected Set<Long> selectSpi(String column, SelectOperator operator,
			Object... values) {
		return memory.select(column, operator, values);
	}

	/**
	 * Check if the commitlog has entered the overflow protection region and if
	 * so, log a WARN message.
	 */
	private void checkForOverflow() {
		if(isFull()) {
			log.warn(
					"The commitlog has exceeded its usable capacity of {} and is now in the overflow prevention region. There are {} bytes left in this region. If these bytes are consumed an oveflow exception will be thrown.",
					usableCapacity, buffer.remaining());
		}
	}

	/**
	 * Add a commit.
	 * 
	 * @param commit
	 * @return {@code true}
	 */
	private boolean commit(Commit commit) {
		synchronized (buffer) {
			Preconditions
					.checkState(
							buffer.remaining() > commit.size() + 4,
							"The commitlog does not have enough capacity to store the commit. The commitlog has %s bytes remaining and the commit requires %s bytes. Consider increasing the value of PCT_CAPACITY_FOR_OVERFLOW_PROTECTION.",
							buffer.remaining(), commit.size() + 4);
			buffer.putInt(commit.size());
			buffer.put(commit.getBytes());
		}
		synchronized (memory) {
			memory.add(commit);
		}
		size += commit.size() + FIXED_SIZE_PER_COMMIT;
		checkForOverflow();
		return true;
	}

	/**
	 * Encapsulates information about a revision for a {@link Value} in the
	 * {@link Cell} at the intersection of a {@link Row} and {@link Column}.
	 * 
	 * @author jnelson
	 */
	@Immutable
	public static final class Commit implements Persistable {

		/**
		 * Return a forStorage commit that corresponds to a revision for
		 * {@code value} in the {@code cell} at the intersection of {@code row}
		 * and {@code column}.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 * @return the revision
		 * @see {@link Value#isForStorage()}
		 */
		public static Commit forStorage(long row, String column, Object value) {
			return new Commit(Key.fromLong(row), column,
					Value.forStorage(value));
		}

		/**
		 * Return the commit represented by {@code bytes}. Use this method when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 * @return the value
		 */
		public static Commit fromByteSequence(ByteBuffer buffer) {
			Key row = Key.fromLong(buffer.getLong());

			int columnSize = buffer.getInt();
			byte[] col = new byte[columnSize];
			buffer.get(col);
			String column = ByteBuffers.getString(ByteBuffer.wrap(col));

			int valueSize = buffer.getInt();
			byte[] val = new byte[valueSize];
			buffer.get(val);
			Value value = Value.fromByteSequence(ByteBuffer.wrap(val));

			return new Commit(row, column, value);
		}

		/**
		 * Return a notForStorage commit that corresponds to a revision for
		 * {@code value} in the {@code cell} at the intersection of {@code row}
		 * and {@code column}.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 * @return the revision
		 * @see {@link Value#isNotForStorage()}
		 */
		public static Commit notForStorage(long row, String column, Object value) {
			Commit commit;
			commit = cache.get(row, column, value);
			if(commit == null) {
				commit = new Commit(Key.fromLong(row), column,
						Value.notForStorage(value));
				cache.put(commit, row, column, value);
			}
			return commit;
		}

		private static final int FIXED_SIZE_IN_BYTES = 2 * (Integer.SIZE / 8); // columnSize,
																				// valueSize

		/**
		 * The average minimum size of a commit in bytes (assumes a column name
		 * of about about 25 characters).
		 */
		public static final int AVG_MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES
				+ Value.MIN_SIZE_IN_BYTES + 50;
		private static final ObjectReuseCache<Commit> cache = new ObjectReuseCache<Commit>();

		private final Key row;
		private final int columnSize;
		private final String column;
		private final int valueSize;
		private final Value value;

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		private Commit(Key row, String column, Value value) {
			this.row = row;
			this.column = column;
			this.columnSize = this.column.getBytes(ByteBuffers.charset()).length;
			this.value = value;
			this.valueSize = this.value.size();
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Commit) {
				Commit other = (Commit) obj;
				return Objects.equal(row, other.row)
						&& Objects.equal(column, other.column)
						&& Objects.equal(value, other.value);
			}
			return false;
		}

		@Override
		public byte[] getBytes() {
			return asByteBuffer().array();
		}

		/**
		 * Return {@code column}.
		 * 
		 * @return the column
		 */
		public String getColumn() {
			return column;
		}

		/**
		 * Return {@code row}.
		 * 
		 * @return the row
		 */
		public Key getRow() {
			return row;
		}

		/**
		 * Return {@code value}.
		 * 
		 * @return the value
		 */
		public Value getValue() {
			return value;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(row, column, value);
		}

		@Override
		public int size() {
			return FIXED_SIZE_IN_BYTES + columnSize + valueSize + row.size();
		}

		@Override
		public String toString() {
			return Strings.toString(this);
		}

		@Override
		public void writeTo(FileChannel channel) throws IOException {
			Writer.write(this, channel);

		}

		/**
		 * Return a new byte buffer that contains the commit with the following
		 * order:
		 * <ol>
		 * <li><strong>rowKey</strong> - first 8 bytes</li>
		 * <li><strong>columnSize</strong> - next 4 bytes</li>
		 * <li><strong>column</strong> - next columnSize bytes</li>
		 * <li><strong>valueSize</strong> - next 4 bytes</li>
		 * <li><strong>value</strong> - remaining bytes</li>
		 * </ol>
		 * 
		 * @return a byte buffer.
		 */
		private ByteBuffer asByteBuffer() {
			ByteBuffer buffer = ByteBuffer.allocate(size());
			buffer.put(row.getBytes());
			buffer.putInt(columnSize);
			buffer.put(column.getBytes(ByteBuffers.charset()));
			buffer.putInt(valueSize);
			buffer.put(value.getBytes());
			buffer.rewind();
			return buffer;
		}
	}

	/**
	 * The representation of the {@link CommitLog} in memory. This is used to
	 * speed up read/write operations. This representation requires 3X the space
	 * that the commitlog uses on disk.
	 * 
	 * @author jnelson
	 */
	private static final class Memory {

		/**
		 * Return a new {@link Memory} with enough capacity for the
		 * {@code expectedSize}.
		 * 
		 * @param expectedSize
		 * @return the memory representation
		 */
		public static Memory newInstancewithExpectedSize(int expectedSize) {
			List<Commit> ordered = Lists.newArrayListWithCapacity(expectedSize);
			Map<Commit, Integer> counts = Maps
					.newHashMapWithExpectedSize(expectedSize);
			return new Memory(ordered, counts);
		}

		private static final Comparator<Value> comp = new Comparator<Value>() {

			@Override
			public int compare(Value o1, Value o2) {
				return o1.compareToLogically(o2);
			}

		};

		private List<Commit> ordered;
		private Map<Commit, Integer> counts;
		private Map<String, TreeMap<Value, Set<Key>>> columns;

		/**
		 * Construct a new instance.
		 * 
		 * @param ordered
		 * @param counts
		 */
		private Memory(List<Commit> ordered, Map<Commit, Integer> counts) {
			this.ordered = ordered;
			this.counts = counts;
			this.columns = new HashMap<String, TreeMap<Value, Set<Key>>>();
		}

		/**
		 * Add the {@code commit}.
		 * 
		 * @param commit
		 */
		public void add(Commit commit) {
			int count = count(commit) + 1;
			counts.put(commit, count);
			ordered.add(commit);
			index(commit); // I won't deindex commits in-memory because it is
							// expensive and I can will check the commit
							// #count() whenever I read from the index.
		}

		/**
		 * Return the count for {@code commit} in the commitlog.
		 * 
		 * @param commit
		 * @return the count
		 */
		public int count(Commit commit) {
			return counts.containsKey(commit) ? counts.get(commit) : 0;
		}

		/**
		 * Return {@code true} if {@code commit} has been committed an odd
		 * number of time.
		 * 
		 * @param commit
		 * @return {@code true} if {@code commit} exists
		 */
		public boolean exists(Commit commit) {
			return Numbers.isOdd(count(commit));
		}

		/**
		 * Return a list of the commits in order.
		 * 
		 * @return the commits
		 */
		public List<Commit> getCommits() {
			return ordered;
		}

		/**
		 * Implement the interface for
		 * {@link CommitLog#selectSpi(String, com.cinchapi.concourse.api.Queryable.SelectOperator, Object...)}
		 * 
		 * @param column
		 * @param operator
		 * @param values
		 * @return the rows that satisfy the select criteria
		 */
		public Set<Long> select(String column, SelectOperator operator,
				Object... values) {
			// Throughout this method I have to check if the value indexed in
			// #columns still exists because I do not deindex columns for
			// removal commits
			Set<Long> rows = Sets.newHashSet();

			Value val = Value.notForStorage(values[0]);

			if(operator == SelectOperator.EQUALS) {
				Set<Key> keys = columns.get(column).get(val);
				Object obj = val.getQuantity();
				for (Key key : keys) {
					long row = key.asLong();
					Commit commit = Commit.notForStorage(row, column, obj);
					if(exists(commit)) {
						rows.add(row);
					}
				}
			}
			else if(operator == SelectOperator.NOT_EQUALS) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					if(!theVal.equals(val)) {
						Set<Key> keys = entry.getValue();
						for (Key key : keys) {
							long row = key.asLong();
							Commit commit = Commit.notForStorage(row, column,
									obj);
							if(exists(commit)) {
								rows.add(key.asLong());
							}
						}
					}
				}
			}
			else if(operator == SelectOperator.GREATER_THAN) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.tailMap(val, false).entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Set<Key> keys = entry.getValue();
					for (Key key : keys) {
						long row = key.asLong();
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
							rows.add(key.asLong());
						}
					}
				}
			}
			else if(operator == SelectOperator.GREATER_THAN_OR_EQUALS) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.tailMap(val, true).entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Set<Key> keys = entry.getValue();
					for (Key key : keys) {
						long row = key.asLong();
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
							rows.add(key.asLong());
						}
					}
				}
			}
			else if(operator == SelectOperator.LESS_THAN) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.headMap(val, false).entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Set<Key> keys = entry.getValue();
					for (Key key : keys) {
						long row = key.asLong();
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
							rows.add(key.asLong());
						}
					}
				}
			}
			else if(operator == SelectOperator.LESS_THAN_OR_EQUALS) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.headMap(val, true).entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Set<Key> keys = entry.getValue();
					for (Key key : keys) {
						long row = key.asLong();
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
							rows.add(key.asLong());
						}
					}
				}
			}
			else if(operator == SelectOperator.BETWEEN) {
				Preconditions
						.checkArgument(values.length > 1,
								"You must specify two arguments for the BETWEEN selector.");
				Value v2 = Value.notForStorage(values[1]);
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.subMap(val, true, v2, false).entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Set<Key> keys = entry.getValue();
					for (Key key : keys) {
						long row = key.asLong();
						Commit commit = Commit.notForStorage(row, column, obj);
						if(exists(commit)) {
							rows.add(key.asLong());
						}
					}
				}
			}
			else if(operator == SelectOperator.REGEX) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Pattern p = Pattern.compile(values[0].toString());
					Matcher m = p.matcher(theVal.toString());
					Set<Key> keys = entry.getValue();
					if(m.matches()) {
						for (Key key : keys) {
							long row = key.asLong();
							Commit commit = Commit.notForStorage(row, column,
									obj);
							if(exists(commit)) {
								rows.add(key.asLong());
							}
						}
					}
				}
			}
			else if(operator == SelectOperator.NOT_REGEX) {
				Iterator<Entry<Value, Set<Key>>> it = columns.get(column)
						.entrySet().iterator();
				while (it.hasNext()) {
					Entry<Value, Set<Key>> entry = it.next();
					Value theVal = entry.getKey();
					Object obj = theVal.getQuantity();
					Pattern p = Pattern.compile(values[0].toString());
					Matcher m = p.matcher(theVal.toString());
					Set<Key> keys = entry.getValue();
					if(!m.matches()) {
						for (Key key : keys) {
							long row = key.asLong();
							Commit commit = Commit.notForStorage(row, column,
									obj);
							if(exists(commit)) {
								rows.add(key.asLong());
							}
						}
					}
				}
			}
			else {
				throw new UnsupportedOperationException(operator
						+ " operator is unsupported");
			}
			return rows;
		}

		/**
		 * Add indexes for the commit to allow for more efficient
		 * {@link #select(String, com.cinchapi.concourse.api.Queryable.SelectOperator, Object...)}
		 * operations.
		 * 
		 * @param commit
		 */
		private void index(Commit commit) {
			String column = commit.getColumn();
			Value value = commit.getValue();
			Key row = commit.getRow();
			TreeMap<Value, Set<Key>> values;
			if(columns.containsKey(column)) {
				values = columns.get(column);
			}
			else {
				values = Maps.newTreeMap(comp);
				columns.put(column, values);
			}
			Set<Key> rows;
			if(values.containsKey(value)) {
				rows = values.get(value);
			}
			else {
				rows = Sets.newHashSet();
				values.put(value, rows);
			}
			rows.add(row);
		}
	}

}
