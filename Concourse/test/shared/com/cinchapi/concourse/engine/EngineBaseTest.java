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
package com.cinchapi.concourse.engine;

import com.cinchapi.concourse.BaseTest;
import com.cinchapi.concourse.engine.Cell;
import com.cinchapi.concourse.engine.ConcourseService;
import com.cinchapi.concourse.engine.Key;
import com.cinchapi.concourse.engine.Row;
import com.cinchapi.concourse.engine.Value;
import com.cinchapi.concourse.engine.Write;
import com.cinchapi.concourse.engine.WriteType;

/**
 * Base class for all tests in the <tt>com.cinchapi.concourse.db</tt> package.
 * 
 * @author jnelson
 */
public abstract class EngineBaseTest extends BaseTest {

	private static final String ROW_HOME = "test/concourse/ds";

	/**
	 * Return a random string that is a valid column name.
	 * 
	 * @return a random column name
	 */
	protected String randomColumnName() {
		String string = randomString().replace(" ", "");
		try {
			ConcourseService.checkColumnName(string);
			return string;
		}
		catch (IllegalArgumentException e) {
			return randomColumnName();
		}
	}

	/**
	 * Return a random new Cell
	 * 
	 * @return the cell
	 */
	protected Cell randomNewCell() {
		return Cell.newInstance(randomColumnName());
	}

	/**
	 * Return a random cell that has been populated with a random number of
	 * revisions.
	 * 
	 * @return the cell
	 */
	protected Cell randomPopulatedCell() {
		Cell cell = randomNewCell();
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Value value = null;
			while (value == null || cell.contains(value)) {
				value = randomValueForStorage();
			}
			cell.add(value);
			if(rand.nextInt() % 3 == 0) {
				cell.remove(value);
				cell.add(value);
			}
			if(rand.nextInt() % 6 == 0) {
				cell.remove(value);
			}
		}
		return cell;
	}

	/**
	 * Return a random new Row
	 * 
	 * @return the row
	 */
	protected Row randomNewRow() {
		return Row.identifiedBy(randomKey(), ROW_HOME);
	}

	/**
	 * Return a random Key.
	 * 
	 * @return the key
	 */
	protected Key randomKey() {
		return Key.fromLong(randomLong());
	}

	/**
	 * Return a random forStorage value.
	 * 
	 * @return the value
	 */
	protected Value randomValueForStorage() {
		return Value.forStorage(randomObject());
	}

	/**
	 * Return a random notForStorage value.
	 * 
	 * @return the value
	 */
	protected Value randomValueNotForStorage() {
		return Value.notForStorage(randomObject());
	}

	/**
	 * Return a random forStorage write.
	 * 
	 * @return the write
	 */
	protected Write randomWriteForStorage() {
		return Write
				.forStorage(
						randomColumnName(),
						randomObject(),
						randomLong(),
						WriteType.values()[rand.nextInt(WriteType.values().length - 1)]); // using
																							// -1
																							// so
																							// that
																							// WriteType.NOT_FOR_STORAGE
																							// isn't
																							// picked
	}

	/**
	 * Return a random notForStorage write.
	 * 
	 * @return the write
	 */
	protected Write randomWriteNotForStorage() {
		return Write.notForStorage(randomColumnName(), randomObject(),
				randomLong());
	}

}
