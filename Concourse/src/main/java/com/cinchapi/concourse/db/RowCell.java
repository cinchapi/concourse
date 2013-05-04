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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.cinchapi.common.lock.Lock;
import com.cinchapi.common.lock.Lockable;
import com.cinchapi.common.lock.Lockables;

/**
 * A {@link Row} based {@link Cell} that is is identified by a {@code column}
 * name and maintains a collection of {@link Value} objects.
 * 
 * @author jnelson
 */
public class RowCell extends Cell<ColumnName, Value> implements Lockable {

	/**
	 * Return the cell represented by {@code bytes}. Use this method when
	 * reading and reconstructing from a file. This method assumes that
	 * {@code bytes} was generated using {@link #getBytes()}.
	 * 
	 * @param bytes
	 * @return the cell
	 */
	static RowCell fromByteSequence(ByteBuffer bytes) {
		return new RowCell(bytes);
	}

	/**
	 * Return a <em>new</em> cell for storage under {@code column}, with a clean
	 * state and no history.
	 * 
	 * @return the new instance
	 */
	static RowCell newInstance(String column) {
		return new RowCell(ColumnName.fromString(column));
	}

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	private RowCell(ByteBuffer bytes) {
		super(bytes);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param column
	 */
	private RowCell(ColumnName column) {
		super(column);
	}

	@Override
	public Lock readLock() {
		return Lockables.readLock(this, lock);
	}

	@Override
	public Lock writeLock() {
		return Lockables.writeLock(this, lock);
	}

	@Override
	protected ColumnName getIdFromByteSequence(ByteBuffer bytes) {
		return ColumnName.fromBytes(bytes.array());
	}

	@Override
	protected Value getObjectFromByteSequence(ByteBuffer bytes) {
		return Value.fromByteSequence(bytes);
	}

}
