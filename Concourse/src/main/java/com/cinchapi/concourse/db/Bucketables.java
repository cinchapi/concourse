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

import java.util.Comparator;

import com.google.common.primitives.Longs;

/**
 * Utility methods for {@link Bucketable} objects.
 * 
 * @author jnelson
 */
public abstract class Bucketables {

	/**
	 * A comparator that sorts objects based on timestamp.
	 */
	private static Comparator<Bucketable> comparator = new Comparator<Bucketable>() {

		@Override
		public int compare(Bucketable o1, Bucketable o2) {
			// push notForStorage objects to the back so that we
			// are sure to reach forStorage values
			if(o1.isNotForStorage()) {
				return o1.equals(o2) ? 0 : 1;
			}
			else if(o1.isNotForStorage()) {
				return this.equals(o2) ? 0 : -1;
			}
			else {
				return -1 * Longs.compare(o1.getTimestamp(), o2.getTimestamp());
			}
		}

	};

	/**
	 * Return {@code true} if the timestamp associated with {@code object} is
	 * not equal to {@link #Storable()#NIL}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is forStorage
	 */
	public static boolean isForStorage(Bucketable object) {
		return object.getTimestamp() != Bucketable.NIL;
	}

	/**
	 * Return {@code true} if the timestamp associated with {@code object} is
	 * equal to {@link #Storable()#NIL}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is notForStorage
	 */
	public static boolean isNotForStorage(Bucketable object) {
		return object.getTimestamp() == Bucketable.NIL;
	}

	/**
	 * Compare to storable objects by timestamp.
	 * 
	 * @param o1
	 * @param o2
	 * @return a negative integer, zero, or a positive integer as this object is
	 *         less than, equal to, or greater than the specified object.
	 */
	public static <O extends Bucketable> int compare(O o1, O o2) {
		return comparator.compare(o1, o2);
	}

}