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
package org.cinchapi.concourse.server.model.legacy;

import java.util.Comparator;

import org.cinchapi.concourse.annotate.UtilityClass;

import com.google.common.primitives.Longs;

/**
 * Tools for {@link Storable} objects.
 * 
 * @author jnelson
 */
@UtilityClass
public final class Storables {

	/**
	 * A comparator that sorts objects in descending order based on timestamp.
	 */
	private static Comparator<Storable> comparator = new Comparator<Storable>() {

		@Override
		public int compare(Storable o1, Storable o2) {
			// push notForStorage objects to the back so that we
			// are sure to reach forStorage values
			if(o1.isNotForStorage() && o2.isNotForStorage()) {
				// FIXME: if both objects are notForStorage then the sorting
				// order should be based on the logical values. Unfortunately
				// there is no way to get the logical value sorting order based
				// on the Storable interface.
				return o1.equals(o2) ? 0 : 0;
			}
			if(o1.isNotForStorage()) {
				return 1;
			}
			else if(o2.isNotForStorage()) {
				return -1;
			}
			else {
				return -1 * Longs.compare(o1.getTimestamp(), o2.getTimestamp());
			}
		}
	};

	/**
	 * Return {@code true} if the timestamp associated with {@code object} is
	 * not equal to {@link Storable#NO_TIMESTAMP}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is forStorage
	 */
	public static boolean isForStorage(Storable object) {
		return object.getTimestamp() != Storable.NO_TIMESTAMP;
	}

	/**
	 * Return {@code true} if the timestamp associated with {@code object} is
	 * equal to {@link Storable#NO_TIMESTAMP}.
	 * 
	 * @param object
	 * @return {@code true} if {@code object} is notForStorage
	 */
	public static boolean isNotForStorage(Storable object) {
		return object.getTimestamp() == Storable.NO_TIMESTAMP;
	}

	/**
	 * Compare {@code o1} to {@code o2} in descending order using their
	 * respective timestamps.
	 * 
	 * @param o1
	 * @param o2
	 * @return a negative integer, zero, or a positive integer as {@code o1} is
	 *         less than, equal to, or greater than {@code o2}.
	 */
	public static <O extends Storable> int compare(O o1, O o2) {
		return comparator.compare(o1, o2);
	}

	private Storables() {/* Utility Class */}

}
