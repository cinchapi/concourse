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
package com.cinchapi.concourse.structure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.Strings;
import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.io.Persistable;
import com.google.common.base.Objects;

/**
 * <p>
 * A {@link Persistable} representation for a revision involving a {@link Value}
 * in the {@link Cell} at the intersection of a {@link Row} and {@link Column}.
 * A single Commit holds information about a row ({@link Key}), column (name),
 * and Value.
 * </p>
 * 
 * @author jnelson
 */
@Immutable
public final class Commit implements Persistable {

	/**
	 * Return a forStorage commit that corresponds to a revision for
	 * {@code value} in the {@code cell} at {@code row}:{@code column}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return the Commit
	 * @see {@link Value#isForStorage()}
	 */
	public static Commit forStorage(String column, Object value, long row) {
		return new Commit(column, Value.forStorage(value), Key.fromLong(row));
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

		return new Commit(column, value, row);
	}

	/**
	 * Return a notForStorage commit that corresponds to a revision for
	 * {@code value} in the {@code cell} at {@code row}:{@code column}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * 
	 * @return the revision
	 * @see {@link Value#isNotForStorage()}
	 */
	public static Commit notForStorage(String column, Object value, long row) {
		Commit commit;
		commit = cache.get(row, column, value);
		if(commit == null) {
			commit = new Commit(column, Value.notForStorage(value),
					Key.fromLong(row));
			cache.put(commit, row, column, value);
		}
		return commit;
	}

	/**
	 * Return a copy of a {@code commit} that is guaranteed to be notForStorage.
	 * 
	 * @param commit
	 * @return a copy of the commit
	 */
	public static Commit notForStorageCopy(Commit commit) {
		return commit.isForStorage() ? Commit.notForStorage(commit.getColumn(),
				commit.getValue().getQuantity(), commit.getRow().asLong())
				: commit;
	}

	private static final int FIXED_SIZE_IN_BYTES = 2 * (Integer.SIZE / 8); // columnSize,
																			// valueSize

	/**
	 * The average minimum size of a commit in bytes (assumes a column name of
	 * about about 25 characters).
	 */
	public static final int AVG_MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES
			+ Value.MIN_SIZE_IN_BYTES + 50;
	private static final ObjectReuseCache<Commit> cache = new ObjectReuseCache<Commit>();

	private final Key row;
	private final int columnSize;
	private final String column;
	private final int valueSize;
	private final Value value;
	private transient int hashCode = 0;

	/**
	 * Construct a new instance.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 */
	private Commit(String column, Value value, Key row) {
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
		if(hashCode == 0) {
			hashCode = Objects.hashCode(row, column, value);
		}
		return hashCode;
	}

	/**
	 * Return {@code true} if the commit is forStorage, meaning it represents a
	 * forStorage value.
	 * 
	 * @return {@code true} if the commit is forStorage
	 */
	public boolean isForStorage() {
		return value.isForStorage();
	}

	/**
	 * Return {@code true} if the commit is notForStorage, meaning it represents
	 * a notForStorage value.
	 * 
	 * @return {@code true} if the commit is notForStorage
	 */
	public boolean isNotForStorage() {
		return value.isNotForStorage();
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