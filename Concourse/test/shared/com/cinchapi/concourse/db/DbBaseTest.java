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

import com.cinchapi.concourse.BaseTest;
import com.cinchapi.concourse.db.ConcourseService;
import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.db.Value;

/**
 * Base class for all tests in the <tt>com.cinchapi.concourse.db</tt> package.
 * 
 * @author jnelson
 */
public abstract class DbBaseTest extends BaseTest {

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
		return Write.forStorage(randomColumnName(), randomObject(),
				randomLong(),
				WriteType.values()[rand.nextInt(WriteType.values().length)]);
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
