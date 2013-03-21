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
package com.cinchapi.concourse.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.Strings;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.config.ConcourseConfiguration.PrefsKey;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.cinchapi.concourse.io.Persistable;
import com.cinchapi.concourse.structure.Commit;
import com.google.common.base.Preconditions;

/**
 * <p>
 * A file mapped {@link VolatileDatabase} whose purpose is to speed up writes
 * and reads for new data in a larger system.
 * </p>
 * <p>
 * Writes are immediately flushed to a file on disk (this has little overhead
 * because the entire capacity of the file is mapped in memory and data is
 * always appended) but reads happen entirely in memory. From time to time, the
 * CommitLog is flushed to a permanent database.
 * </p>
 * <p>
 * The size of the CommitLog on disk cannot exceed {@value #MAX_SIZE_IN_BYTES}
 * bytes, but it will take up up to 4X more space in memory.
 * </p>
 * 
 * 
 * @author jnelson
 */
public class CommitLog extends VolatileDatabase implements
		IterableByteSequences,
		Persistable {

	/**
	 * Return the CommitLog represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param buffer
	 * @param populated
	 *            - specify as {@code true} if the buffer has been populated
	 *            from an existing commitlog, set to {@code false} otherwise for
	 *            an empty commitlog
	 * @return the commitLog
	 */
	public static CommitLog fromByteSequences(MappedByteBuffer buffer,
			boolean populated) {
		return new CommitLog(buffer, populated);
	}

	/**
	 * Return the CommitLog that is stored in the file at {@code location}. This
	 * function will fail if the file does not exist.
	 * 
	 * @param location
	 *            - path to the FILE (not directory) to use for the CommitLog
	 * @param config
	 * @return the CommitLog stored at {@code location}
	 */
	public static CommitLog fromFile(String location,
			ConcourseConfiguration config) {
		int oldSize = (int) new File(location).length();
		int size = config.getInt(Prefs.COMMMIT_LOG_SIZE_IN_BYTES,
				DEFAULT_SIZE_IN_BYTES);
		Preconditions
				.checkArgument(
						size >= oldSize,
						"The commitlog at %s is too large to be loaded. "
								+ "Incrase the value of COMMMIT_LOG_SIZE_IN_BYTES to %s",
						location, oldSize);
		Preconditions.checkState(size < MAX_SIZE_IN_BYTES,
				"The size of the CommitLog cannot be greater than %s bytes",
				MAX_SIZE_IN_BYTES);
		MappedByteBuffer buffer = Utilities.openBuffer(location, size);
		return CommitLog.fromByteSequences(buffer, true);
	}

	/**
	 * Return a new CommitLog instance at {@code location}. This commit log will
	 * overwrite anything that was previously written to the file at
	 * {@code location}.
	 * 
	 * @param location
	 *            - path to the FILE (not directory) to use for the CommitLog
	 * @param config
	 * @return the new commit log
	 */
	public static CommitLog newInstance(String location,
			ConcourseConfiguration config) {
		int size = config.getInt(Prefs.COMMMIT_LOG_SIZE_IN_BYTES,
				DEFAULT_SIZE_IN_BYTES);
		Preconditions.checkArgument(size < MAX_SIZE_IN_BYTES,
				"The size of the CommitLog cannot be greater than %s bytes",
				MAX_SIZE_IN_BYTES);
		try {
			File tmp = new File(location);
			tmp.delete(); // delete the old file if it was there
			tmp.getParentFile().mkdirs();
			tmp.createNewFile();
			MappedByteBuffer buffer = Utilities.openBuffer(location, size);
			return CommitLog.fromByteSequences(buffer, false);
		}
		catch (IOException e) {
			throw new ConcourseRuntimeException(e);
		}

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
	 * The default filename for the commitlog.
	 */
	public static final String DEFAULT_LOCATION = "commitlog";

	/**
	 * The maximum allowable size of the CommitLog on disk.
	 */
	public static final int MAX_SIZE_IN_BYTES = (Integer.MAX_VALUE - 10);

	/**
	 * The default size of the commitlog
	 */
	public static final int DEFAULT_SIZE_IN_BYTES = MAX_SIZE_IN_BYTES - 1;

	private static final Logger log = LoggerFactory.getLogger(CommitLog.class);
	private static final double PCT_USABLE_CAPACITY = 100 - PCT_CAPACITY_FOR_OVERFLOW_PREVENTION;

	/**
	 * <strong>Note</strong>: Each commit is preceded by a 4 byte int specifying
	 * the size.
	 */
	private final MappedByteBuffer buffer;
	private int size = 0;
	private final int usableCapacity;

	/**
	 * 
	 * Construct a new instance from the {@link MappedByteBuffer} of an existing
	 * file.
	 * 
	 * @param buffer
	 * @param populated
	 *            - specify as {@code true} if the buffer has been populated
	 *            from an existing commitlog, set to {@code false} otherwise for
	 *            an empty commitlog
	 */
	private CommitLog(MappedByteBuffer buffer, boolean populated) {
		super(buffer.capacity() / (Commit.AVG_MIN_SIZE_IN_BYTES + 4)); // I'm
																		// adding
																		// 4 to
																		// account
																		// for
																		// the 4
																		// bytes
																		// used
																		// to
																		// store
																		// the
																		// size
																		// for
																		// each
																		// commit
		this.buffer = buffer;
		this.usableCapacity = (int) Math.round((PCT_USABLE_CAPACITY / 100.0)
				* buffer.capacity());

		if(populated) {
			byte[] bytes = new byte[buffer.capacity()];
			this.buffer.get(bytes);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(bytes);
			while (bsit.hasNext()) {
				commit(Commit.fromByteSequence(bsit.next())); // this will
																// only
																// record
																// the
																// commit in
																// memory
																// and
																// not the
																// underlying
																// file
																// (because
																// its
																// already
																// there!)
			}
			this.buffer.position(bsit.position());
		}
	}

	/**
	 * <p>
	 * <strong>USE WITH CAUTION!</strong>
	 * </p>
	 * <p>
	 * Return an iterator that should only be used for flushing the commitlog.
	 * Each call to {@link Iterator#next} will DELETE the returned commit.
	 * </p>
	 * 
	 * @return the iterator
	 */
	public Iterator<Commit> flusher() {
		synchronized (this) {
			return new Iterator<Commit>() {

				int expectedCount = ordered.size();

				@Override
				public boolean hasNext() {
					return !ordered.isEmpty();
				}

				@Override
				public Commit next() {
					checkForComodification();
					Commit next = ordered.remove(0); // authorized
					int count = counts.get(next) - 1;
					if(count == 0) {
						counts.remove(next); // authorized
					}
					else {
						counts.put(next, count); // authorized
					}
					int nextSize = next.size() + 4;
					buffer.position(buffer.position() + nextSize);
					buffer.compact();
					expectedCount--;
					size -= nextSize;
					return next;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();

				}

				/**
				 * Check for concurrent modification to the commit log.
				 */
				private void checkForComodification() {
					if(expectedCount != ordered.size()) {
						throw new ConcurrentModificationException(
								"Attempted modification to CommitLog while flushing");
					}
				}

			};
		}
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
	public synchronized void shutdown() {
		buffer.force();
		log.info("Successfully shutdown the CommitLog.");
		super.shutdown();
		
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
	protected boolean addSpi(String column, Object value, long row) {
		return append(Commit.forStorage(column, value, row));
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		return append(Commit.forStorage(column, value, row));
	}

	/**
	 * Append {@code commit} to the underlying file and perform the
	 * {@link #commit(Commit)} function so that it is also added to the volatile
	 * store.
	 * 
	 * @param commit
	 * @return {@code true}
	 */
	/*
	 * (non-Javadoc)
	 * This method does not override #commit because it is necessary to access a
	 * distinct method for only altering the VolatileDatabase (i.e. when
	 * constructing a CommitLog from an existing file)
	 */
	private boolean append(Commit commit) {
		synchronized (buffer) {
			// Must attempt to write to the file before writing to memory
			Preconditions
					.checkState(
							buffer.remaining() > commit.size() + 4,
							"The commitlog does not have enough capacity to store the commit. "
									+ "The commitlog has %s bytes remaining and the commit requires %s bytes. "
									+ "Consider increasing the value of PCT_CAPACITY_FOR_OVERFLOW_PROTECTION.",
							buffer.remaining(), commit.size() + 4);
			buffer.putInt(commit.size());
			buffer.put(commit.getBytes());
			buffer.force();
		}
		size += commit.size() + FIXED_SIZE_PER_COMMIT;
		checkForOverflow();
		return super.commit(commit);
	}

	/**
	 * Check if the commitlog has entered the overflow protection region and if
	 * so, log a WARN message.
	 */
	private void checkForOverflow() {
		if(isFull()) {
			log.warn(
					"The commitlog has exceeded its usable capacity of {} bytes and is now "
							+ "in the overflow prevention region. There are {} bytes left in "
							+ "this region. If these bytes are consumed an oveflow exception "
							+ "will be thrown.", usableCapacity,
					buffer.remaining());
		}
	}

	/**
	 * The configurable preferences used in this class.
	 */
	private enum Prefs implements PrefsKey {
		COMMMIT_LOG_SIZE_IN_BYTES
	}

	/**
	 * CommitLog utility methods
	 */
	private static class Utilities {

		/**
		 * Open a {@link MappedByteBuffer} with {@code size} for the existing
		 * file at {@code location}.
		 * 
		 * @param location
		 * @param size
		 * @return the byte buffer
		 */
		public static MappedByteBuffer openBuffer(String location, int size) {
			try {
				FileChannel channel = new RandomAccessFile(location, "rw")
						.getChannel();
				MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, 0,
						size);
				channel.close();
				return buffer;
			}
			catch (IOException e) {
				throw new ConcourseRuntimeException(e);
			}
		}
	}

}
