/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import org.cinchapi.concourse.util.RandomStringGenerator;


/**
 * 
 * 
 * @author jnelson
 */
public abstract class Random {

	private static final java.util.Random rand = new java.util.Random();
	private static final RandomStringGenerator strand = new RandomStringGenerator();

	/**
	 * Return a random boolean value.
	 * 
	 * @return the boolean.
	 */
	public static boolean getBoolean() {
		int seed = rand.nextInt();
		if(seed % 2 == 0) {
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Return a random double value.
	 * 
	 * @return the double.
	 */
	public static double getDouble() {
		return rand.nextDouble();
	}

	/**
	 * Return a random float value.
	 * 
	 * @return the float.
	 */
	public static float getFloat() {
		return rand.nextFloat();
	}

	/**
	 * Return a random integer value.
	 * 
	 * @return the int.
	 */
	public static int getInt() {
		return rand.nextInt();
	}

	/**
	 * Return a random long value.
	 * 
	 * @return the long.
	 */
	public static long getLong() {
		return rand.nextLong();
	}

	/**
	 * Return a random negative number.
	 * 
	 * @return the number.
	 */
	public static Number getNegativeNumber() {
		int seed = getInt();
		if(seed % 5 == 0) {
			return (float) -1 * Math.abs(getFloat());
		}
		else if(seed % 4 == 0) {
			return (double) -1 * Math.abs(getDouble());
		}
		else if(seed % 3 == 0) {
			return (long) -1 * Math.abs(getLong());
		}
		else {
			return (int) -1 * Math.abs(getInt());
		}
	}

	/**
	 * Return a random number value.
	 * 
	 * @return the number
	 */
	public static Number getNumber() {
		int seed = getInt();
		if(seed % 5 == 0) {
			return getFloat();
		}
		else if(seed % 4 == 0) {
			return getDouble();
		}
		else if(seed % 3 == 0) {
			return getLong();
		}
		else {
			return getInt();
		}
	}

	/**
	 * Return a random object value.
	 * 
	 * @return the object
	 */
	public static Object getObject() {
		int seed = rand.nextInt();
		if(seed % 5 == 0) {
			return getBoolean();
		}
		else if(seed % 2 == 0) {
			return getNumber();
		}
		else {
			return getString();
		}
	}

	/**
	 * Return a random positive long value.
	 * 
	 * @return the long
	 */
	public static Number getPositiveNumber() {
		int seed = getInt();
		if(seed % 5 == 0) {
			return Math.abs(getFloat());
		}
		else if(seed % 4 == 0) {
			return (double) Math.abs(getDouble());
		}
		else if(seed % 3 == 0) {
			return (long) Math.abs(getLong());
		}
		else {
			return (int) Math.abs(getInt());
		}
	}

	/**
	 * Pause execution for a random number of milliseconds between 0 and 1
	 * second.
	 */
	public static void sleep() {
		try {
			Thread.sleep(rand.nextInt(1000) + 1); // between 0 and 1 second
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static int getScaleCount() {
		return rand.nextInt(90) + 10;
	}

	/**
	 * Return a get string, possibly with digits.
	 * 
	 * @return the string
	 */
	public static String getString() {
		return strand.nextStringAllowDigits();
	}

	/**
	 * Return a get string, with no digits.
	 * 
	 * @return the string.
	 */
	public static String getStringNoDigits() {
		return strand.nextString();
	}

}
