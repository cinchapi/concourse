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

import com.cinchapi.concourse.db.ColumnName;

/**
 * 
 * 
 * @author jnelson
 */
public class RowCellTest extends CellTest<ColumnName, Value> {

	@Override
	protected Value forStorageObject() {
		return randomValueForStorage();
	}

	@Override
	protected ColumnName id() {
		return ColumnName.fromString(randomColumnName());
	}

	@Override
	protected Cell<ColumnName, Value> newInstance(ColumnName id) {
		return newRowCell(id.toString());
	}

	@Override
	protected Value notForStorageObject() {
		return randomValueNotForStorage();
	}

	@Override
	protected Cell<ColumnName, Value> populatedInstanceFromBytes(
			ByteBuffer bytes) {
		return RowCell.fromByteSequence(bytes);
	}

	@Override
	protected Value copy(Value object) {
		return Value.forStorage(object.getQuantity());
	}

}
