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
package com.cinahpi.concourse;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.RandomString;
import com.cinchapi.common.time.StopWatch;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.db.Cell;
import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.db.Row;
import com.cinchapi.concourse.db.Value;

import junit.framework.TestCase;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Base class for all Concourse related unit tests.
 * 
 * @author jnelson
 */
@SuppressWarnings("unused")
// used by subclasses
public abstract class BaseTest extends TestCase {

	/*
	 * Loggers are normally static, but I'm defining it as a member here because
	 * I don't want to define a new one for each subclass. I'm OK with doing
	 * this for test classes.
	 */
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final Random rand = new Random();
	private final RandomString strand = new RandomString();

	protected int minScaleFreq = 10;
	protected int maxScaleFreq = 100;

	/**
	 * Return the current timestamp from {@link Time#now()}.
	 * 
	 * @return the current timestamp in microseconds
	 */
	protected final long getCurrentTime() {
		return Time.now();
	}

	/**
	 * Return the {@link Logger}.
	 * 
	 * @return the logger
	 */
	protected final Logger getLogger() {
		return log;
	}

	/**
	 * Return the {@link Random} number generator.
	 * 
	 * @return the random
	 */
	protected final Random getRandom() {
		return rand;
	}

	/**
	 * Return the {@link RandomString} generator.
	 * 
	 * @return the random string
	 */
	protected final RandomString getRandomString() {
		return strand;
	}

	/**
	 * Return the frequency to use for scale tests.
	 * 
	 * @return the frequency.
	 */
	protected final int getScaleFrequency() {
		return rand.nextInt(maxScaleFreq - minScaleFreq) + minScaleFreq;
	}

	/**
	 * Log a message at the INFO level.
	 * 
	 * @param format
	 * @param arguments
	 * @see {@link Logger#info(String, Object...)}.
	 */
	protected void log(String format, Object... arguments) {
		log.info(format, arguments);
	}

	protected void mockValueForStorage() {
		Value v = Mockito.mock(Value.class);
		long timestamp = Time.now();
		Object quantity = randomObject();
		Mockito.when(v.getQuantity()).thenReturn(quantity);
		Mockito.when(v.getTimestamp()).thenReturn(timestamp);
		Mockito.when(v.isForStorage()).thenReturn(true);
	}

	/**
	 * Return a random {@link Key}.
	 * 
	 * @return the key
	 */
	protected Key randomKey() {
		return Key.fromLong(randomLong());
	}

	/**
	 * Return a new {@link Cell} at a random {@code row} and {@code column}.
	 * 
	 * @return the cell
	 */
	protected Cell randomNewCell() {
		Key row = randomKey();
		String column = randomString();
		return Cell.newInstance(row, column);
	}

	/**
	 * Return a new {@link Row} with a random {@code key}.
	 * 
	 * @return the row
	 */
	protected Row randomNewRow() {
		Key key = randomKey();
		return Row.newInstance(key);
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
	 * Return a random {@link Value} where {@link Value#isForStorage()} is
	 * {@code true}.
	 * 
	 * @return the value.
	 */
	protected Value randomValueForStorage() {
		return new ValueBuilder().build();
	}

	/**
	 * Return a random {@link Value} where {@link Value#isForStorage()} is
	 * {@code false}.
	 * 
	 * @return the value.
	 */
	protected Value randomValueNotForStorage() {
		return new ValueBuilder().forStorage(false).build();
	}

	/**
	 * A builder for {@link Value} objects.
	 * 
	 * @author jnelson
	 */
	protected class ValueBuilder {

		private Object quantity = randomObject();
		private boolean forStorage = true;

		/**
		 * Construct a new instance.
		 */
		public ValueBuilder() {}

		/**
		 * Build the {@link Value}.
		 * 
		 * @return the value
		 */
		public Value build() {
			return forStorage ? Value.forStorage(quantity) : Value
					.notForStorage(quantity);
		}

		/**
		 * If {@code true} then the built {@link Value} will return {@code true}
		 * for {@link Value#isForStorage()}. Default is {@code true}.
		 * 
		 * @param quantity
		 * @return this
		 */
		public ValueBuilder forStorage(boolean forStorage) {
			this.forStorage = forStorage;
			return this;
		}

		/**
		 * Set the {@code quantity} for the {@link Value} that will be built.
		 * Default is random.
		 * 
		 * @param quantity
		 * @return this
		 */
		public ValueBuilder quantity(Object quantity) {
			this.quantity = quantity;
			return this;
		}
	}

}
