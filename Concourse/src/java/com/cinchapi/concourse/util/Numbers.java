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

import java.math.BigDecimal;

/**
 * {@link Number} related utility methods.
 * 
 * @author jnelson
 */
public final class Numbers {
	
	/**
	 * Compare {@code a} to {@code b}.
	 * @param a
	 * @param b
	 * @return -1, 0, or 1 as {@code a} is numerically less than, equal to, or greater than {@code b}.
	 */
	public static int compare(Number a, Number b){
		BigDecimal first = new BigDecimal(a.toString());
		BigDecimal second = new BigDecimal(b.toString());
		return first.compareTo(second);
	}

	/**
	 * Return {@code true} if {@code num} is evenly divisible by two.
	 * 
	 * @param num
	 * @return {@code true} if {@code num} is even.
	 */
	public static boolean isEven(Number num) {
		return num.intValue() % 2 == 0;
	}

	/**
	 * Return {@code true} if {@code num} is not evenly divisible by
	 * two.
	 * 
	 * @param num
	 * @return {@code true} if {@code num} is odd.
	 */
	public static boolean isOdd(Number num) {
		return !isEven(num);
	}

}
