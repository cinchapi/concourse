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

/**
 * 
 * 
 * @author jnelson
 */
public class RowCell extends Cell<UTF8String, Value> {

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
	static RowCell newInstance(UTF8String column) {
		return new RowCell(column);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param column
	 */
	private RowCell(UTF8String column) {
		super(column);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	private RowCell(ByteBuffer bytes) {
		super(bytes);
	}

	@Override
	protected Value extractObject(ByteBuffer bytes) {
		return Value.fromByteSequence(bytes);
	}

	@Override
	protected UTF8String extractId(ByteBuffer bytes) {
		return new UTF8String(bytes.array());
	}

}
