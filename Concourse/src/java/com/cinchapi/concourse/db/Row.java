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
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Objects;

import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.io.IterableByteSequences;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.db.io.Persistable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>
 * Represents a {@link Cell} set where each is mapped from a {@link Column}
 * name. A row can hold up to {@value #MAX_NUM_CELLS} cells throughout its
 * lifetime, but in actuality this limit is lower because the size of a cell can
 * very widely and is guaranteed to increase with every revision. Therefore it
 * is more useful to use the maximum allowable storage for a row as a guide.
 * <h2>Storage Requirements</h2>
 * <ul>
 * <li>An empty row occupies {@value #MIN_SIZE_IN_BYTES} bytes.</li>
 * <li>A row, and all of its cells, cannot occupy more than
 * {@value #MAX_SIZE_IN_BYTES} bytes.</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 */
public class Row implements IterableByteSequences, Persistable {

	/**
	 * Return the row represented by {@code bytes}. Use this method when reading
	 * and reconstructing from a file. This method assumes that {@code bytes}
	 * was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the value
	 */
	public static Row fromByteSequence(ByteBuffer buffer) {
		int size = buffer.getInt();
		Key key = Key.fromLong(buffer.getLong());
		int cellCount = buffer.getInt();

		List<Section> cells = Lists.newArrayListWithExpectedSize(cellCount);
		byte[] cellBytes = new byte[size - FIXED_SIZE_IN_BYTES];
		buffer.get(cellBytes);

		IterableByteSequences.ByteSequencesIterator bsit = IterableByteSequences.ByteSequencesIterator
				.over(cellBytes);
		while (bsit.hasNext()) {
			cells.add(Section.fromByteSequence((bsit.next())));
		}
		return new Row(key, cells);
	}

	/**
	 * Return an empty row identified by {@code key}.
	 * 
	 * @param key
	 * @return the row.
	 */
	public static Row newInstance(Key key) {
		List<Section> cells = Lists.newArrayList();
		return new Row(key, cells);
	}

	private static final int FIXED_SIZE_IN_BYTES = 2 * (Integer.SIZE / 8)
			+ Long.SIZE / 8; // size, cellCount, key

	/**
	 * The maximum allowable size of a row.
	 */
	public static final int MAX_SIZE_IN_BYTES = Integer.MAX_VALUE;

	/**
	 * The minimum size of a row (e.g an empty row with no cells).
	 */
	public static final int MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES;

	/**
	 * The maximum number of cells that can exist in a row. In actuality, this
	 * limit is much lower because the size of a cell can very widely.
	 */
	public static final int MAX_NUM_CELLS = MAX_SIZE_IN_BYTES
			/ Cell.MIN_SIZE_IN_BYTES;

	private int count = 0;
	private List<Section> sections;
	private final Key key;

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param sections
	 */
	private Row(Key key, List<Section> sections) {
		this.key = key;
		this.sections = sections;
	}

	/**
	 * Add {@code value} to the cell under {@code column}.
	 * 
	 * @param column
	 * @param value
	 */
	public synchronized void add(String column, Value value) {
		Section section = Section.newInstance(key, column);
		if(sections.contains(section)) {
			section = sections.get(sections.indexOf(section));
		}
		else {
			sections.add(section);
			setCount(count + 1);
		}
		section.getCell().add(value);
	}

	/**
	 * Return the columns that have non-empty cells.
	 * 
	 * @return
	 */
	public synchronized Set<String> columnSet() {
		return columnSet(Time.now());
	}

	/**
	 * Return the columns that had non-empty cell right before {@code at}.
	 * 
	 * @param at
	 * @return the columns.
	 */
	public synchronized Set<String> columnSet(long at) {
		Iterator<Section> it = sections.iterator();
		Set<String> columns = Sets.newTreeSet();
		while (it.hasNext()) {
			Section section = it.next();
			if(!section.getCell().getValues(at).isEmpty()) {
				columns.add(section.getColumn());
			}
		}
		return columns;
	}

	/**
	 * Return all the columns that have cells (both empty and non-empty).
	 * 
	 * @return the columns.
	 */
	public synchronized Set<String> columnSetAll() {
		Set<String> columns = Sets.newTreeSet();
		Iterator<Section> it = sections.iterator();
		while (it.hasNext()) {
			columns.add(it.next().getColumn());
		}
		return columns;
	}

	/**
	 * Return the number of non-empty cells.
	 * 
	 * @return the count.
	 */
	public synchronized int count() {
		return count;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Row) {
			Row other = (Row) obj;
			return Objects.equal(this.key, other.key)
					&& Objects.equal(this.sections, other.sections)
					&& Objects.equal(this.count, other.count);
		}
		return false;
	}

	/**
	 * Get the cell under {@code column}, even if the cell is empty.
	 * 
	 * @param column
	 * @return the cell.
	 */
	@Nullable
	public synchronized Cell get(String column) {
		try {
			return sections
					.get(sections.indexOf(Section.forComparison(column)))
					.getCell();
		}
		catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	@Override
	public synchronized byte[] getBytes() {
		return asByteBuffer().array();
	}

	/**
	 * Return the {@code key} that identifies this row.
	 * 
	 * @return the {@code key}.
	 */
	public Key getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(key, sections, count);
	}

	/**
	 * Return {@code true} if the row is empty.
	 * 
	 * @return {@code true} if there are 0 non-empty cells in the row.
	 */
	public synchronized boolean isEmpty() {
		return count == 0;
	}

	@Override
	public synchronized ByteSequencesIterator iterator() {
		return IterableByteSequences.ByteSequencesIterator.over(getBytes());
	}

	/**
	 * Remove {@code value} from the the cell under {@code column}.
	 * 
	 * @param column
	 * @param value
	 */
	public synchronized void remove(String column, Value value) {
		Section section = Section.newInstance(key, column);
		if(sections.contains(section)) {
			section = sections.get(sections.indexOf(section));
			section.getCell().remove(value);
			if(section.getCell().isEmpty()) {
				setCount(count - 1);
			}
		}
	}

	/**
	 * Remove any existing values from the cell under {@code column} and add
	 * {@code value} to the cell.
	 * 
	 * @param column
	 * @param value
	 */
	public synchronized void set(String column, Value value) {
		Section section = Section.newInstance(key, column);
		if(sections.contains(section)) {
			section = sections.get(sections.indexOf(section));
		}
		section.getCell().removeAll(); // calling the Cell#removeAll() method
										// directly bypasses the local #remove()
										// method, which adjusts the #count
										// appropriately, but this is OK because
										// the #set() operation has a net
										// neutral affect on the #count
		section.getCell().add(value);
	}

	@Override
	public synchronized int size() {
		return asByteBuffer().capacity();
	}

	@Override
	public String toString() {
		return key + ": " + sections;
	}

	@Override
	public void writeTo(FileChannel channel) throws IOException {
		Writer.write(this, channel);

	}

	/**
	 * Return a new byte buffer that contains the value with the following
	 * order:
	 * <ol>
	 * <li><strong>size</strong> - first 4 bytes</li>
	 * <li><strong>key</strong> - next 8 bytes</li>
	 * <li><strong>cellCount</strong> - next 4 bytes</li>
	 * <li><strong>cells</strong> - remaining bytes</li>
	 * </ol>
	 * The bytes that represent the cells will conform to
	 * {@link IterableByteSequences}.
	 * 
	 * @return a byte buffer.
	 */
	private ByteBuffer asByteBuffer() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (Section section : sections) {
			byte[] bytes = section.getBytes();
			try {
				out.write(ByteBuffer.allocate(4).putInt(bytes.length).array()); // for
																				// some
																				// reason
																				// writing
																				// the
																				// length
																				// of
																				// the
																				// array
																				// doesn't
																				// work
																				// properly,
																				// so
																				// I
																				// have
																				// to
																				// wrap
																				// it
																				// in
																				// a
																				// byte
																				// buffer
																				// :-/
				out.write(bytes);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		byte[] cellBytes = out.toByteArray();
		int size = FIXED_SIZE_IN_BYTES + cellBytes.length;

		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.putInt(size);
		buffer.putLong(key.asLong());
		buffer.putInt(sections.size());
		buffer.put(cellBytes);

		return ByteBuffer.wrap(out.toByteArray());
	}

	/**
	 * Thread-safe method to set {@link #count}.
	 * 
	 * @param size
	 */
	private synchronized void setCount(int size) {
		this.count = size;
	}

	/**
	 * Encapsulates the mapping from a {@link Column} name to the corresponding
	 * {@link Cell} in this row.
	 * 
	 * @author jnelson
	 */
	@Immutable
	private static final class Section implements Persistable {

		/**
		 * Return the section represented by {@code bytes}. Use this method when
		 * reading and reconstructing from a file. This method assumes that
		 * {@code bytes} was generated using {@link #getBytes()}.
		 * 
		 * @param bytes
		 * @return the section
		 */
		public static Section fromByteSequence(ByteBuffer bytes) {
			int columnSize = bytes.getInt();
			int cellSize = bytes.getInt();

			byte[] col = new byte[columnSize];
			bytes.get(col);
			String column = ByteBuffers.getString(ByteBuffer.wrap(col));

			byte[] cel = new byte[cellSize];
			bytes.get(cel);
			Cell cell = Cell.fromByteSequence(ByteBuffer.wrap(cel));

			return new Section(column, cell);
		}

		/**
		 * Return a new instance for the cell at the intersection of {@code row}
		 * and {@code column}.
		 * 
		 * @param row
		 * @param column
		 * @return
		 */
		public static Section newInstance(Key row, String column) {
			Cell cell = Cell.newInstance(row, column);
			return new Section(column, cell);
		}

		/**
		 * <p>
		 * Return an instance that will be used for comparisons. This method
		 * utilizes an {@link ObjectReuseCache} to prevent unnecessary object
		 * duplication for sections that will not be stored.
		 * </p>
		 * <p>
		 * <strong>Note:</strong> This method encapsulates a dummy cell, so it
		 * is NOT suitable for ANY storage.
		 * </p>
		 * 
		 * @param column
		 * @return
		 */
		public static Section forComparison(String column) {
			Section section = cache.get(column);
			if(section == null) {
				section = new Section(column, Cell.newInstance(
						comparisonInstanceKey, column));
				cache.put(section, column);
			}
			return section;

		}

		private static final int fixedSizeInBytes = 2 * (Integer.SIZE / 8); // columnSize,
																			// cellSize
		private static final ObjectReuseCache<Section> cache = new ObjectReuseCache<Section>();
		private static final Key comparisonInstanceKey = Key.fromLong(0L);

		private final String column;
		private final int columnSize;
		private Cell cell;

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param cell
		 */
		public Section(String column, Cell cell) {
			this.column = column;
			this.columnSize = this.column.getBytes(ByteBuffers.charset()).length;
			this.cell = cell;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Section) {
				Section other = (Section) obj;
				return Objects.equal(column, other.column); // I don't care
															// about the Cell
															// state here
															// because this
															// method is called
															// whenever I create
															// a new Cellsection
															// and try to find
															// the cannoical
															// Cellsection in
															// the Row's
															// collection using
															// contains()
			}
			return false;
		}

		@Override
		public byte[] getBytes() {
			return asByteBuffer().array();
		}

		/**
		 * Return the encapsulated {@code cell}.
		 * 
		 * @return the cell
		 */
		public Cell getCell() {
			return cell;
		}

		/**
		 * Return the encapsulated {@code column} name.
		 * 
		 * @return the column
		 */
		public String getColumn() {
			return column;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(column);
		}

		@Override
		public int size() {
			synchronized (cell) {
				return fixedSizeInBytes + columnSize + cell.size();
			}
		}

		@Override
		public String toString() {
			return "Cell under column '" + column + "': " + cell;
		}

		@Override
		public void writeTo(FileChannel channel) throws IOException {
			Writer.write(this, channel);

		}

		/**
		 * Return a new byte buffer that contains the section with the following
		 * order:
		 * <ol>
		 * <li><strong>columnSize</strong> - first 4 bytes</li>
		 * <li><strong>cellSize</strong> - next 4 bytes</li>
		 * <li><strong>column</strong> - next columnSize bytes</li>
		 * <li><strong>cell</strong> - remaining bytes</li>
		 * </ol>
		 * 
		 * @return a byte buffer.
		 */
		private ByteBuffer asByteBuffer() {
			synchronized (cell) {
				ByteBuffer buffer = ByteBuffer.allocate(size());
				buffer.putInt(columnSize);
				buffer.putInt(cell.size());
				buffer.put(ByteBuffers.toByteBuffer(column));
				buffer.put(cell.getBytes());
				buffer.rewind();
				return buffer;
			}
		}
	}
}
