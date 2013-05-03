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

import com.cinchapi.concourse.db.RowCell.ByteSizedString;

/**
 * 
 * 
 * @author jnelson
 */
public class RowCellTest extends CellTest<ByteSizedString, Value> {

	@Override
	protected Value forStorageObject() {
		return randomValueForStorage();
	}

	@Override
	protected ByteSizedString id() {
		return ByteSizedString.fromString(randomColumnName());
	}

	@Override
	protected Cell<ByteSizedString, Value> newInstance() {
		return randomNewRowCell();
	}

	@Override
	protected Cell<ByteSizedString, Value> newInstance(ByteSizedString id) {
		return newRowCell(id.toString());
	}

	@Override
	protected Value notForStorageObject() {
		return randomValueNotForStorage();
	}

	@Override
	protected Cell<ByteSizedString, Value> populatedInstance() {
		return randomPopulatedRowCell();
	}

	@Override
	protected Cell<ByteSizedString, Value> populatedInstanceFromBytes(
			ByteBuffer bytes) {
		return RowCell.fromByteSequence(bytes);
	}

	@Override
	protected Value copy(Value object) {
		return Value.forStorage(object.getQuantity());
	}

}
