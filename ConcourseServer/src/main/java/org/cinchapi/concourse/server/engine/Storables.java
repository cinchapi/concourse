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
package org.cinchapi.concourse.server.engine;

import java.util.Comparator;

import org.cinchapi.common.annotate.PackagePrivate;

import com.google.common.primitives.Longs;

/**
 * Utility methods for {@link Storable} objects.
 * 
 * @author jnelson
 */
@PackagePrivate
abstract class Storables {

	/**
	 * A comparator that sorts objects based on timestamp.
	 */
	private static Comparator<Storable> comparator = new Comparator<Storable>() {

		@Override
		public int compare(Storable o1, Storable o2) {
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
	public static boolean isForStorage(Storable object) {
		return object.getTimestamp() != Storable.NIL;
	}

	/**
	 * Return {@code true} if the timestamp associated with {@code object} is
	 * equal to {@link #Storable()#NIL}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is notForStorage
	 */
	public static boolean isNotForStorage(Storable object) {
		return object.getTimestamp() == Storable.NIL;
	}

	/**
	 * Compare to storable objects by timestamp.
	 * 
	 * @param o1
	 * @param o2
	 * @return a negative integer, zero, or a positive integer as this object is
	 *         less than, equal to, or greater than the specified object.
	 */
	public static <O extends Storable> int compare(O o1, O o2) {
		return comparator.compare(o1, o2);
	}

}
