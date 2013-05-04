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
public class ColumnCell extends Cell<Value, Key> {

	/**
	 * Construct a new instance.
	 * 
	 * @param id
	 */
	public ColumnCell(Value id) {
		super(id);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	protected ColumnCell(ByteBuffer bytes) {
		super(bytes);
	}

	@Override
	protected Value getIdFromByteSequence(ByteBuffer bytes) {
		return Value.fromByteSequence(bytes);
	}

	@Override
	protected Key getObjectFromByteSequence(ByteBuffer bytes) {
		return Key.fromByteSequence(bytes);
	}

}
