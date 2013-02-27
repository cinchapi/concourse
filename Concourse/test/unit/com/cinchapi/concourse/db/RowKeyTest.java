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

import java.util.Random;

import org.junit.Test;

import junit.framework.TestCase;

/**
 * Unit tests for {@link Key}.
 * 
 * @author jnelson
 */
public final class RowKeyTest extends TestCase {

	private static Random rand = new Random();

	/**
	 * Return a negative number.
	 * 
	 * @return the number.
	 */
	private long getNegative() {
		return -1 * Math.abs(rand.nextLong());
	}

	/**
	 * Return a positive number.
	 * 
	 * @return the number.
	 */
	private long getPositive() {
		return Math.abs(rand.nextLong());
	}

	@Test
	public void testUnsignedConstraint() {
		long positive = getPositive();
		long negative = getNegative();
		Key positiveKey = Key.create(positive);
		Key negativeKey = Key.create(negative);

		// row keys should never be negative
		assertFalse(Long.toString(positive).startsWith("-"));
		assertFalse(positiveKey.toString().startsWith("-"));
		assertTrue(Long.toString(negative).startsWith("-"));
		assertFalse(negativeKey.toString().startsWith("-"));
	}

}
