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
package com.cinchapi.concourse;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.RandomString;
import com.cinchapi.common.time.StopWatch;
import com.cinchapi.common.time.Time;

import junit.framework.TestCase;

/**
 * Base class for all unit tests that implements commonly needed functionality.
 * 
 * @author jnelson
 */
public abstract class BaseTest extends TestCase {

	/*
	 * Loggers are normally static, but I'm defining it as a member here because
	 * I don't want to define a new one for each subclass. I'm OK with doing
	 * this for test classes.
	 */
	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	protected final Random rand = new Random();
	protected final RandomString strand = new RandomString();
	protected final StopWatch watch = new StopWatch();

	protected int minScaleFreq = 10;
	protected int maxScaleFreq = 100;

	/**
	 * Log a message at the INFO level. To log at a different level, access
	 * {@link #log} directly.
	 * 
	 * @param format
	 * @param arguments
	 * @see {@link Logger#info(String, Object...)}.
	 */
	protected void log(String format, Object... arguments) {
		log.info(format, arguments);
	}

	/**
	 * Return a random boolean value.
	 * 
	 * @return the boolean.
	 */
	protected final boolean randomBoolean() {
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
	protected final double randomDouble() {
		return rand.nextDouble();
	}

	/**
	 * Return a random float value.
	 * 
	 * @return the float.
	 */
	protected final float randomFloat() {
		return rand.nextFloat();
	}

	/**
	 * Return a random integer value.
	 * 
	 * @return the int.
	 */
	protected final int randomInt() {
		return rand.nextInt();
	}

	/**
	 * Return a random long value.
	 * 
	 * @return the long.
	 */
	protected final long randomLong() {
		return rand.nextLong();
	}

	/**
	 * Return a random negative number.
	 * 
	 * @return the number.
	 */
	protected final long randomNegativeLong() {
		return -1 * randomPositiveLong();
	}

	/**
	 * Return a random number value.
	 * 
	 * @return the number
	 */
	protected final Number randomNumber() {
		int seed = randomInt();
		if(seed % 5 == 0) {
			return randomFloat();
		}
		else if(seed % 4 == 0) {
			return randomDouble();
		}
		else if(seed % 3 == 0) {
			return randomLong();
		}
		else {
			return randomInt();
		}
	}

	/**
	 * Return a random object value.
	 * 
	 * @return the object
	 */
	protected final Object randomObject() {
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

	/**
	 * Return a random positive number
	 * 
	 * @return the number.
	 */
	protected final long randomPositiveLong() {
		return Math.abs(rand.nextLong());
	}

	/**
	 * Return the frequency to use for scale tests.
	 * 
	 * @return the frequency.
	 */
	protected final int randomScaleFrequency() {
		return rand.nextInt(maxScaleFreq - minScaleFreq) + minScaleFreq;
	}

	/**
	 * Return a random number of milliseconds to use with Thread.sleep()
	 * 
	 * @return the sleep time
	 */
	protected long randomSleep() {
		return rand.nextInt(1000) + 0; // between 0 and 1 second
	}

	/**
	 * Return a random string, possibly with digits.
	 * 
	 * @return the string
	 */
	protected String randomString() {
		return strand.nextStringAllowDigits();
	}

	/**
	 * Return a random string, with no digits.
	 * 
	 * @return the string.
	 */
	protected String randomStringNoDigits() {
		return strand.nextString();
	}

	/**
	 * Return the current timestamp from {@link Time#now()}.
	 * 
	 * @return the current timestamp in microseconds
	 */
	protected final long time() {
		return Time.now();
	}

}
