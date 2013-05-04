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
public class ColumnCellTest extends CellTest<Value, Key>{

	@Override
	protected Key forStorageObject() {
		return randomKeyForStorage();
	}

	@Override
	protected Value id() {
		return randomValueNotForStorage();
	}

	@Override
	protected Cell<Value, Key> newInstance(Value id) {
		return newColumnCell(id);
	}

	@Override
	protected Key notForStorageObject() {
		return randomKeyNotForStorage();
	}

	@Override
	protected Cell<Value, Key> populatedInstanceFromBytes(ByteBuffer bytes) {
		return ColumnCell.fromByteSequence(bytes);
	}

	@Override
	protected Key copy(Key object) {
		return Key.forStorage(object.asLong());
	}

}
