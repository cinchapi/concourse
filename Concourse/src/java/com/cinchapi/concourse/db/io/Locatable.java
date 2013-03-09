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
package com.cinchapi.concourse.db.io;

import com.cinchapi.common.Hash;

/**
 * An object that can be located using a sequence of bytes.
 * 
 * @author jnelson
 */
public interface Locatable {

	/**
	 * Return the locator.
	 * 
	 * @return the locator.
	 */
	public byte[] getLocator();

	/**
	 * A utility class for creating locators.
	 * 
	 * @author jnelson
	 */
	public static class Locators {

		/**
		 * The size of each {@code locator} is 32 bytes.
		 */
		public final static int SIZE = 32;

		/**
		 * Return a {@code locator} based on the {@code components}.
		 * 
		 * @param components
		 * @return the locator
		 */
		public static byte[] create(byte[]... components) {
			return Hash.sha256(components);
		}
	}

}
