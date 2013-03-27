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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.Strings;
import com.cinchapi.common.cache.ObjectReuseCache;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.io.ByteSized;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * <p>
 * A {@link ByteSized} representation for a write involving a {@link Value} in
 * the {@code cell} at the intersection of a {@code row} and {@code column} that
 * is designed for temporary, append-only storage.
 * </p>
 * 
 * @author jnelson
 */
@Immutable
class Write implements ByteSized {

	/**
	 * Drop {@code write} into {@code file}. Any existing file content will be
	 * deleted.
	 * 
	 * @param write
	 * @param file
	 * @return the dropped write
	 */
	static DroppedWrite drop(Write write, String file) {
		return new DroppedWrite(write, file);
	}

	/**
	 * Return a write that is appropriate for storage and corresponds to a
	 * revision for {@code value} in the {@code cell} at {@code row}:
	 * {@code column}.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @param type
	 * 
	 * @return the new instance
	 * @see {@link Value#isForStorage()}
	 */
	static Write forStorage(String column, Object value, long row,
			WriteType type) {
		Preconditions.checkArgument(type != WriteType.NOT_FOR_STORAGE,
				"Cannot create a forStorage Write with a NOT_FOR_STORAGE type");
		return new Write(column, Value.forStorage(value), Key.fromLong(row),
				type);
	}

	/**
	 * Return the write represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the write
	 */
	static Write fromByteSequence(ByteBuffer bytes) {
		WriteType type = WriteType.values()[bytes.getInt()];

		Key row = Key.fromLong(bytes.getLong());

		int columnSize = bytes.getInt();
		byte[] col = new byte[columnSize];
		bytes.get(col);
		String column = ByteBuffers.getString(ByteBuffer.wrap(col));

		int valueSize = bytes.getInt();
		byte[] val = new byte[valueSize];
		bytes.get(val);
		Value value = Value.fromByteSequence(ByteBuffer.wrap(val));

		return new Write(column, value, row, type);
	}

	/**
	 * Return a write that is not appropriate for storage, but can be used in
	 * comparisons. This is the preferred way to create writes unless the
	 * write
	 * will be stored.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @return the new instance.
	 */
	static Write notForStorage(String column, Object value, long row) {
		Write write = cache.get(row, column, value);
		if(write == null) {
			write = new Write(column, Value.notForStorage(value),
					Key.fromLong(row), WriteType.NOT_FOR_STORAGE);
			cache.put(write, row, column, value);
		}
		return write;
	}

	/**
	 * Return a copy of a {@code write} that is guaranteed to be notForStorage.
	 * 
	 * @param write
	 * @return the copy
	 */
	static Write notForStorageCopy(Write write) {
		return write.isForStorage() ? Write.notForStorage(write.getColumn(),
				write.getValue().getQuantity(), write.getRow().asLong())
				: write;
	}

	private static final int FIXED_SIZE_IN_BYTES = 3 * (Integer.SIZE / 8); // type,
																			// columnSize,
																			// valueSize
	/**
	 * The average minimum size of a write in bytes.
	 * <em>Assumes a column name of about about 12 characters</em>.
	 */
	public static final int AVG_MIN_SIZE_IN_BYTES = FIXED_SIZE_IN_BYTES
			+ Value.MIN_SIZE_IN_BYTES + Column.AVG_COLUMN_NAME_SIZE_IN_BYTES;
	private static final ObjectReuseCache<Write> cache = new ObjectReuseCache<Write>();

	private final Key row;
	private final int columnSize;
	private final String column;
	private final int valueSize;
	private final Value value;
	private final WriteType type;
	private transient int hashCode = 0;

	/**
	 * Construct a new instance.
	 * 
	 * @param column
	 * @param value
	 * @param row
	 * @param type
	 */
	protected Write(String column, Value value, Key row, WriteType type) {
		this.row = row;
		this.column = column;
		this.columnSize = this.column.getBytes(ByteBuffers.charset()).length;
		this.value = value;
		this.valueSize = this.value.size();
		this.type = type;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Write) {
			Write other = (Write) obj;
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

	@Override
	public int hashCode() {
		if(hashCode == 0) {
			hashCode = Objects.hashCode(row, column, value);
		}
		return hashCode;
	}

	@Override
	public int size() {
		return FIXED_SIZE_IN_BYTES + columnSize + valueSize + row.size();
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	/**
	 * Return {@code column}.
	 * 
	 * @return the column
	 */
	String getColumn() {
		return column;
	}

	/**
	 * Return {@code row}.
	 * 
	 * @return the row
	 */
	Key getRow() {
		return row;
	}

	/**
	 * Return the {@code type}.
	 * 
	 * @return the type
	 */
	WriteType getType() {
		return type;
	}

	/**
	 * Return {@code value}.
	 * 
	 * @return the value
	 */
	Value getValue() {
		return value;
	}

	/**
	 * Return {@code true} if the write is forStorage, meaning it represents a
	 * forStorage value.
	 * 
	 * @return {@code true} if the write is forStorage
	 */
	boolean isForStorage() {
		return value.isForStorage();
	}

	/**
	 * Return {@code true} if the write is notForStorage, meaning it represents
	 * a notForStorage value.
	 * 
	 * @return {@code true} if the write is notForStorage
	 */
	boolean isNotForStorage() {
		return value.isNotForStorage();
	}

	/**
	 * Return a new byte buffer that contains the write with the following
	 * order:
	 * <ol>
	 * <li><strong>type</strong> - first 4 bytes</li>
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
		buffer.putInt(type.ordinal());
		buffer.put(row.getBytes());
		buffer.putInt(columnSize);
		buffer.put(column.getBytes(ByteBuffers.charset()));
		buffer.putInt(valueSize);
		buffer.put(value.getBytes());
		buffer.rewind();
		return buffer;
	}
}