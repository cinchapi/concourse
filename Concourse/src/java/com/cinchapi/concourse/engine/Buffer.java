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
package com.cinchapi.concourse.engine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.Strings;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.concourse.config.ConcourseConfiguration;
import com.cinchapi.concourse.config.ConcourseConfiguration.PrefsKey;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.cinchapi.concourse.io.ByteSized;
import com.google.common.base.Preconditions;

/**
 * <p>
 * A disk based {@link ConcourseService} service that aims to offer fast writes
 * and queries at the expense of a large memory footprint<sup>1</sup>.
 * </p>
 * <p>
 * Each write is immediately flushed to a file on disk (this has little overhead
 * because the entire capacity of the file is mapped in memory and data is
 * always appended). Furthermore, no indices are stored on disk which further
 * optimizes writes and also reads because they must happen entirely in memory.
 * When an existing Buffer is loaded from disk, its writes are replayed in
 * memory and the indices are recreated.
 * </p>
 * <p>
 * <sup>1</sup> - The size of the Buffer on disk cannot exceed
 * {@value #MAX_SIZE_IN_BYTES} bytes, but it will take up up to 4X more space in
 * memory.
 * </p>
 * 
 * @author jnelson
 */
class Buffer extends VolatileStorage implements
		FlushableService,
		IterableByteSequences,
		ByteSized {

	/**
	 * Return the Buffer that is stored in the file at {@code location}.
	 * This function will fail if the file does not exist.
	 * 
	 * @param location
	 *            - path to the FILE (not directory) to use for the Buffer
	 * @param config
	 * @return the Buffer stored at {@code location}
	 */
	public static Buffer fromFile(String location, ConcourseConfiguration config) {
		int oldSize = (int) new File(location).length();
		int size = config.getInt(Prefs.BUFFER_SIZE_IN_BYTES,
				DEFAULT_SIZE_IN_BYTES);
		Preconditions
				.checkArgument(
						size >= oldSize,
						"The Buffer at %s is too large to be loaded. "
								+ "Incrase the value of COMMMIT_LOG_SIZE_IN_BYTES to %s",
						location, oldSize);
		Preconditions.checkState(size < MAX_SIZE_IN_BYTES,
				"The size of the Buffer cannot be greater than %s bytes",
				MAX_SIZE_IN_BYTES);

		double pctOverflowProtection = config.getDouble(
				Prefs.PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PROTECTION,
				DEFAULT_PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PREVENTION);
		double pctUsableCapacity = 100 - pctOverflowProtection;

		MappedByteBuffer buffer = Utilities.openBuffer(location, size, false);
		String backup = location + ".bak";
		return fromByteSequences(buffer, pctUsableCapacity, backup, true);
	}

	/**
	 * Return a new Buffer instance at {@code location}. This buffer
	 * will
	 * overwrite anything that was previously written to the file at
	 * {@code location}.
	 * 
	 * @param location
	 *            - path to the FILE (not directory) to use for the Buffer
	 * @param config
	 * @return the new buffer
	 */
	public static Buffer newInstance(String location,
			ConcourseConfiguration config) {
		int size = config.getInt(Prefs.BUFFER_SIZE_IN_BYTES,
				DEFAULT_SIZE_IN_BYTES);
		Preconditions.checkArgument(size < MAX_SIZE_IN_BYTES,
				"The size of the Buffer cannot be greater than %s bytes",
				MAX_SIZE_IN_BYTES);

		double pctOverflowProtection = config.getDouble(
				Prefs.PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PROTECTION,
				DEFAULT_PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PREVENTION);
		double pctUsableCapacity = 100 - pctOverflowProtection;

		MappedByteBuffer buffer = Utilities.openBuffer(location, size, true);
		String backup = location + ".bak";
		return fromByteSequences(buffer, pctUsableCapacity, backup, false);
	}

	/**
	 * Return the Buffer represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param buffer
	 * @param pctUsableCapacity
	 * @param backup
	 * @param populated
	 *            - specify as {@code true} if the buffer has been populated
	 *            from an existing Buffer, set to {@code false} otherwise
	 *            for
	 *            an empty Buffer
	 * @return the Buffer
	 */
	protected static Buffer fromByteSequences(MappedByteBuffer buffer,
			double pctUsableCapacity, String backup, boolean populated) {
		return new Buffer(buffer, pctUsableCapacity, backup, populated);
	}

	private static final int FIXED_SIZE_PER_WRITE = Integer.SIZE / 8; // for
																		// storing
																		// the
																		// size
																		// of
																		// of
																		// the
																		// write

	/**
	 * <p>
	 * The percent of capacity to reserve for overflow protection. This is a
	 * safeguard against cases when the Buffer has M bytes of capacity remaining
	 * and the next write requires N bytes of storage (N > M). Technically the
	 * Buffer is not full, but it would experience an overflow if it tired to
	 * add the commit.
	 * </p>
	 * <p>
	 * Setting this value to X means that a Buffer with a total capacity of Z
	 * will be deemed full when it reaches a capacity of Y (Y = 100-X * Z). Any
	 * capacity < Y is not considered full whereas any capacity > Y is
	 * considered full. So when the Buffer is M bytes away from reaching a
	 * capacity of Y, adding a write that requires N bytes (N > M) will not
	 * cause an overflow so long as N-M < Z-Y.
	 * </p>
	 */
	public static final double DEFAULT_PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PREVENTION = .02;

	/**
	 * The maximum allowable size of the Buffer on disk.
	 */
	public static final int MAX_SIZE_IN_BYTES = (Integer.MAX_VALUE - 10);

	/**
	 * The default size of the Buffer
	 */
	public static final int DEFAULT_SIZE_IN_BYTES = MAX_SIZE_IN_BYTES - 1;

	private static final Logger log = LoggerFactory.getLogger(Buffer.class);

	/**
	 * <strong>Note</strong>: Each write is preceded by a 4 byte int specifying
	 * the size.
	 */
	private final MappedByteBuffer buffer;
	private final String backup;
	private int size = 0;
	private final int usableCapacity;

	/**
	 * 
	 * Construct a new instance from the {@link MappedByteBuffer} of an existing
	 * file.
	 * 
	 * @param buffer
	 * @param pctUsableCapacity
	 * @param backup
	 * @param populated
	 *            - specify as {@code true} if the buffer has been populated
	 *            from an existing Buffer, set to {@code false} for an
	 *            empty
	 *            Buffer
	 */
	private Buffer(MappedByteBuffer buffer, double pctUsableCapacity,
			String backup, boolean populated) {
		super(buffer.capacity()
				/ (Write.AVG_MIN_SIZE_IN_BYTES + FIXED_SIZE_PER_WRITE));
		this.buffer = buffer;
		this.usableCapacity = (int) Math.round((pctUsableCapacity / 100.0)
				* buffer.capacity());
		this.backup = backup;

		if(populated) {
			byte[] bytes = new byte[buffer.capacity()];
			this.buffer.get(bytes);
			IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
					.over(bytes);
			while (bsit.hasNext()) {
				Write write = Write.fromByteSequence(bsit.next());
				// this will only record the write in memory and not the
				// underlying file (because it is already there!)
				commit(write, false);
				size += write.size() + FIXED_SIZE_PER_WRITE;
			}
			this.buffer.position(bsit.position());
			reindex();
		}
		log.info("The buffer is ready.");
	}

	/**
	 * <p>
	 * <strong>USE WITH CAUTION!</strong>
	 * </p>
	 * <p>
	 * Return an iterator that should only be used for flushing the Buffer. Each
	 * call to {@link Iterator#next} will DELETE the returned commit.
	 * </p>
	 * 
	 * @return the iterator
	 */
	public WriteFlusher flusher() {
		return new Flusher();
	}

	@Override
	public byte[] getBytes() {
		ByteBuffer copy = buffer.asReadOnlyBuffer();
		copy.rewind();
		byte[] bytes = new byte[size];
		copy.get(bytes);
		return bytes;
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
		log.info("The buffer has shutdown gracefully.");
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
	protected boolean addSpi(String column, Object value, long row) {
		return append(Write.forStorage(column, value, row, WriteType.ADD), true);
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		return append(Write.forStorage(column, value, row, WriteType.REMOVE),
				false);
	}

	/**
	 * Check to see if a {@link DroppedWrite} exists because the buffer was
	 * interrupted in the middle of being flushed.
	 * 
	 * @return the dropped write or {@code null}
	 */
	@Nullable
	DroppedWrite checkForDroppedWrite() {
		File backupFile = new File(backup);
		if(backupFile.exists()) {
			DroppedWrite write = DroppedWrite.fromFile(backupFile);
			if(!write.isIdenticalTo(ordered.get(0))) {
				return write;
			}
			else {
				write.discard(); // the buffer was ungracefully shutdown before
									// the dropped write was removed so it
									// can be discarded since its still in the
									// buffer and is still ready to be flushed
			}
		}
		return null;
	}

	/**
	 * Return {@code true} if the buffer has exceeded its usable capacity.
	 * 
	 * @return {@code true} if the buffer is full
	 */
	boolean isFull() {
		return size >= usableCapacity;
	}

	/**
	 * Return {@code true} if the buffer is too full to write the revision
	 * for {@code value} in {@code row}: {@code column}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return {@code true} if the buffer is full
	 */
	boolean isFull(String column, Object value, long row) {
		Write write = Write.notForStorage(column, value, row);
		return size + write.size() + FIXED_SIZE_PER_WRITE > usableCapacity;
	}

	/**
	 * Append {@code write} to the underlying file and perform the
	 * {@link #commit(Write)} function so that it is also added to the volatile
	 * store.
	 * 
	 * @param write
	 * @param index
	 * @return {@code true}
	 * @see {@link #commit(Write, boolean)}
	 */
	/*
	 * (non-Javadoc)
	 * This method does not override #commit because it is necessary to have a
	 * distinct method for only altering the VolatileDatabase (i.e. when
	 * constructing a Buffer from an existing file)
	 */
	private boolean append(Write write, boolean index) {
		synchronized (buffer) {
			Preconditions
					.checkState(
							buffer.remaining() >= write.size() + 4,
							"The buffer does not have enough capacity to store "
									+ "the commit. The buffer has %s bytes remaining and "
									+ "the write requires %s bytes. Consider increasing the "
									+ "value of PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PROTECTION "
									+ "or the value of BUFFER_SIZE_IN_BYTES.",
							buffer.remaining(), write.size() + 4);
			buffer.putInt(write.size());
			buffer.put(write.getBytes());
			buffer.force();
		}
		size += write.size() + FIXED_SIZE_PER_WRITE;
		checkForOverflow();
		return super.commit(write, index);
	}

	/**
	 * Check if the Buffer has entered the overflow protection region and
	 * if
	 * so, log a WARN message.
	 */
	private void checkForOverflow() {
		if(isFull()) {
			log.warn("The buffer has exceeded its usable capacity of {} bytes "
					+ "and is now in the overflow prevention region. "
					+ "There are {} bytes left in this region. If these "
					+ "bytes are consumed an oveflow exception will be "
					+ "thrown.", usableCapacity, buffer.remaining());
		}
	}

	/**
	 * An flushing iterator, that removes writes from the mapped file and the
	 * in-memory indices for each call to next();
	 * 
	 * @author jnelson
	 */
	private class Flusher implements WriteFlusher {
		int expectedCount = ordered.size();

		/**
		 * Construct a new instance.
		 */
		public Flusher() {
			buffer.rewind();
		}

		@Override
		public void ack() {
			Utilities.deleteFile(backup);
		}

		@Override
		public boolean hasNext() {
			return !ordered.isEmpty();
		}

		@Override
		public Write next() {
			checkForComodification();
			Write next = ordered.remove(0); // authorized
			int count = counts.get(next) - 1;
			if(count == 0) {
				counts.remove(next); // authorized
			}
			else {
				counts.put(next, count); // authorized
			}
			int nextSize = next.size() + FIXED_SIZE_PER_WRITE;
			Write.drop(next, backup);
			buffer.position(buffer.position() + nextSize);
			buffer.compact();
			buffer.position(0);
			expectedCount--;
			size -= nextSize;
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		/**
		 * Check for concurrent modification to the buffer.
		 */
		private void checkForComodification() {
			if(expectedCount != ordered.size()) {
				throw new ConcurrentModificationException(
						"Attempted modification to Buffer while flushing");
			}
		}

	}

	/**
	 * The configurable preferences used in this class.
	 */
	private enum Prefs implements PrefsKey {
		BUFFER_SIZE_IN_BYTES, PCT_CAPACITY_FOR_BUFFER_OVERFLOW_PROTECTION
	}

	/**
	 * Buffer utility methods
	 */
	private static class Utilities {

		/**
		 * Delete the file at {@code location}.
		 * 
		 * @param location
		 */
		public static void deleteFile(String location) {
			File tmp = new File(location);
			tmp.delete();
		}

		/**
		 * Open a {@link MappedByteBuffer} with {@code size} for the existing
		 * file at {@code location}.
		 * 
		 * @param location
		 * @param size
		 * @param clean
		 * @return the byte buffer
		 */
		public static MappedByteBuffer openBuffer(String location, int size,
				boolean clean) {
			try {
				if(clean) {
					clean(location);
				}
				FileChannel channel = new RandomAccessFile(location, "rwd")
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

		/**
		 * Clean the file at {@code location}
		 * 
		 * @param location
		 */
		private static void clean(String location) {
			try {
				File tmp = new File(location);
				tmp.delete();
				tmp.getParentFile().mkdirs();
				tmp.createNewFile();
			}
			catch (IOException e) {
				throw new ConcourseRuntimeException(e);
			}
		}
	}

}
