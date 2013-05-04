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
	 * @param id
	 */
	public ColumnCell(Value id) {
		super(id);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Construct a new instance.
	 * @param bytes
	 */
	protected ColumnCell(ByteBuffer bytes) {
		super(bytes);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Cell#getIdFromBytes(java.nio.ByteBuffer)
	 */
	@Override
	protected Value getIdFromBytes(ByteBuffer bytes) {
		return Value.fromByteSequence(bytes);
	}

	/* (non-Javadoc)
	 * @see com.cinchapi.concourse.db.Cell#getObjectFromBytes(java.nio.ByteBuffer)
	 */
	@Override
	protected Key getObjectFromBytes(ByteBuffer bytes) {
		// TODO Auto-generated method stub
		return null;
	}

}
