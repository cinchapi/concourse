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

import org.junit.Test;
import com.cinahpi.concourse.BaseTest;

/**
 * Unit tests for {@link Key}.
 * 
 * @author jnelson
 */
public final class KeyTest extends BaseTest {


	@Test
	public void testUnsignedConstraint() {
		long positive = randomPositiveLong();
		long negative = randomNegativeLong();
		Key positiveKey = Key.fromLong(positive);
		Key negativeKey = Key.fromLong(negative);

		// row keys should never be negative
		assertFalse(Long.toString(positive).startsWith("-"));
		assertFalse(positiveKey.toString().startsWith("-"));
		assertTrue(Long.toString(negative).startsWith("-"));
		assertFalse(negativeKey.toString().startsWith("-"));
	}

}
