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
package com.cinchapi.concourse.temp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.Strings;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.api.ConcourseService;
import com.cinchapi.concourse.db.Value;
import com.cinchapi.concourse.io.Persistable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * <p>
 * A temporary {@link ConcourseService} that is fully represented in memory
 * using a {@link HeapDatabase} and also persisted on disk in a append-only
 * memory mapped file. Occasionally, the CommitLog is flushed to a permanent
 * database. The size of the CommitLog on disk cannot exceed
 * {@value #MAX_SIZE_IN_BYTES} bytes, but it will take up up to 4X more space in
 * memory.
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

		HeapDatabase memory = HeapDatabase
				.newInstancewithExpectedSize(bytes.length
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
		HeapDatabase memory = HeapDatabase.newInstancewithExpectedSize(size
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

	private final HeapDatabase memory;
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
	private CommitLog(MappedByteBuffer buffer, HeapDatabase memory) {
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

}
