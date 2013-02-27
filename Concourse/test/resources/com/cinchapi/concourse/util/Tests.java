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
package com.cinchapi.concourse.util;

import java.util.Random;

import com.cinchapi.concourse.util.Time;
import com.cinchapi.util.AtomicClock;
import com.cinchapi.util.RandomString;

/**
 * Utility methods for tests.
 * 
 * @author jnelson
 */
public class Tests {

	/**
	 * Return the current timestamp.
	 * 
	 * @return the time.
	 */
	public static long currentTime() {
		return Time.now();
	}

	/**
	 * Return a negative number.
	 * 
	 * @return the number.
	 */
	public static long negativeNumber() {
		return -1 * Math.abs(rand.nextLong());
	}

	/**
	 * Return a positive number.
	 * 
	 * @return the number.
	 */
	public static long positiveNumber() {
		return Math.abs(rand.nextLong());
	}

	/**
	 * Return a random boolean.
	 * 
	 * @return the boolean.
	 */
	public static Boolean randomBoolean() {
		int seed = rand.nextInt();
		if(seed % 2 == 0) {
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Return a random number.
	 * 
	 * @return the number.
	 */
	public static Number randomNumber() {
		int seed = rand.nextInt();
		if(seed % 5 == 0) {
			return rand.nextFloat();
		}
		else if(seed % 4 == 0) {
			return rand.nextDouble();
		}
		else if(seed % 3 == 0) {
			return rand.nextLong();
		}
		else {
			return rand.nextInt();
		}
	}

	/**
	 * Return a random integer.
	 * 
	 * @return the int
	 */
	public static int randomInt() {
		return rand.nextInt();
	}

	/**
	 * Return a random integer between 0 (inclusive) and n (exclusive).
	 * 
	 * @param n
	 * @return the int
	 */
	public static int randomInt(int n) {
		return rand.nextInt(n);
	}

	/**
	 * Return a random scale test frequency.
	 * 
	 * @return the frequeny.
	 */
	public static int randomScaleFreq() {
		return rand.nextInt(maxScaleFreq - minScaleFreq) + minScaleFreq;
	}

	/**
	 * Return a random string, possibly with digits.
	 * 
	 * @return the string
	 */
	public static String randomString() {
		return strand.nextStringAllowDigits();
	}

	/**
	 * Return a random string, with no digits.
	 * 
	 * @return the string.
	 */
	public static String randomStringNoDigits() {
		return strand.nextString();
	}

	/**
	 * Return a random value.
	 * 
	 * @return the value.
	 */
	public static Object randomValue() {
		int seed = rand.nextInt();
		if(seed % 5 == 0) {
			return randomBoolean();
		}
		else if(seed % 2 == 0) {
			return randomNumber();
		}
		else {
			return randomString();
		}
	}

	private static Random rand = new Random();
	private static RandomString strand = new RandomString();
	private static int minScaleFreq = 10;
	private static int maxScaleFreq = 100;

	private Tests() {}

}
