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

/**
 * Utility methods for {@link Storable} objects.
 * 
 * @author jnelson
 */
public abstract class Storables {

	/**
	 * Return {@code true} if the timestamp associated with {@code object}
	 * is not equal to {@link #Storable()#NIL}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is forStorage
	 */
	public static boolean isForStorage(Storable object) {
		return object.getTimestamp() != Storable.NIL;
	}

	/**
	 * Return {@code true} if the timestamp associated with {@code object}
	 * is equal to {@link #Storable()#NIL}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is notForStorage
	 */
	public static boolean isNotForStorage(Storable object) {
		return object.getTimestamp() == Storable.NIL;
	}

}