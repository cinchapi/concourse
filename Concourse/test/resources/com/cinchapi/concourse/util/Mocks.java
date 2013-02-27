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

import org.powermock.core.classloader.annotations.PrepareForTest;

import com.cinchapi.concourse.db.Value;

import static org.mockito.Mockito.*;

/**
 * Factory methods to create mock objects for dependency injection.
 * 
 * @author jnelson
 */
@PrepareForTest(Value.class)
public class Mocks {

	/**
	 * Return a mock {@link Value}.
	 * 
	 * @param timestamp
	 * @return the mock object.
	 */
	public static Value getValue(long timestamp) {
		return getValue(Tests.randomValue(), timestamp);
	}

	/**
	 * Return a mock {@link Value}.
	 * 
	 * @param timestamp
	 * @return the mock object.
	 */
	public static Value getValue(Object value, long timestamp) {
		Value v = timestamp != 0 ? Value.forStorage(value, timestamp)
				: Value.notForStorage(value);
		return v;
	}

	/**
	 * Return a mock {@link Value}
	 * 
	 * @return the mock object.
	 */
	public static Value getValueForStorage() {
		return getValue(Tests.currentTime());
	}

	/**
	 * Return a mock {@link Value}
	 * 
	 * @return the mock object.
	 */
	public static Value getValueNotForStorage() {
		return getValue(0);
	}

}
