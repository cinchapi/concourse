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
package com.cinchapi.common.time;


/**
 * Time utilities.
 * 
 * @author jnelson
 */
public final class Time {

	private static AtomicClock clock = new AtomicClock();

	/**
	 * Get the current timestamp. Use this throughout a project to make sure
	 * that no time collisions happen.
	 * 
	 * @return the current timestamp.
	 */
	public static long now() {
		long now;
		synchronized (clock) {
			now = clock.time();
		}
		return now;
	}

}
