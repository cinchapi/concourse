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

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * A basic timer.
 * 
 * @author jnelson
 */
public class Timer {

	private final static int defaultStartTime = 0;
	private final static TimeUnit defaultTimeUnit = TimeUnit.MICROSECONDS;
	private long start = defaultStartTime;

	/**
	 * Start the timer now.
	 */
	public void start() {
		Preconditions.checkState(start == defaultStartTime,
				"The timer has already been started.");
		start = Time.now();
	}

	/**
	 * Stop the timer now and return the amount of time measured in
	 * {@code milliseconds} that has elapsed.
	 * 
	 * @return the elapsed time.
	 */
	public long stop() {
		return stop(defaultTimeUnit);
	}

	/**
	 * Stop the timer now and return the amount of time measured in
	 * {@code unit} that has elapsed.
	 * 
	 * @param unit
	 * @return the elapsed time.
	 */
	public long stop(TimeUnit unit) {
		Preconditions.checkArgument(start != defaultStartTime,
				"The timer has not been started.");
		long duration = Time.now() - start;
		start = defaultStartTime;
		return unit.convert(duration, defaultTimeUnit);
	}

}
