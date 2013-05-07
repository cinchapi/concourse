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
public class ColumnCell extends Bucket<Value, Key> {
	
	public static ColumnCell newInstance(Value value){
		return new ColumnCell(value);
	}
	
	public static ColumnCell fromByteSequence(ByteBuffer bytes){
		return new ColumnCell(bytes);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param id
	 */
	private ColumnCell(Value id) {
		super(id);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param bytes
	 */
	private ColumnCell(ByteBuffer bytes) {
		super(bytes);
	}

	@Override
	protected Value getKeyFromByteSequence(ByteBuffer bytes) {
		return Value.fromByteSequence(bytes);
	}

	@Override
	protected Key getValueFromByteSequence(ByteBuffer bytes) {
		return Key.fromByteSequence(bytes);
	}

}
