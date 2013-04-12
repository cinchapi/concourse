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
package com.cinchapi.common.math;

import java.math.BigDecimal;

import com.google.common.base.Preconditions;

/**
 * {@link Number} related utility methods.
 * 
 * @author jnelson
 */
public final class Numbers {

	/**
	 * Return numerator/denominator as a percent.
	 * 
	 * @param numerator
	 * @param denominator
	 * @return the percent
	 */
	public static double asPercent(long numerator, long denominator) {
		return numerator * 100.0 / denominator;
	}

	/**
	 * Compare {@code a} to {@code b}.
	 * 
	 * @param a
	 * @param b
	 * @return -1, 0, or 1 as {@code a} is numerically less than, equal to, or
	 *         greater than {@code b}.
	 */
	public static int compare(Number a, Number b) {
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
	 * Return {@code true} if {@code num} is not evenly divisible by two.
	 * 
	 * @param num
	 * @return {@code true} if {@code num} is odd.
	 */
	public static boolean isOdd(Number num) {
		return !isEven(num);
	}

	/**
	 * Return the largest int from a list.
	 * 
	 * @param nums
	 * @return the largest int
	 */
	public static int max(int... nums) {
		int max = nums[0];
		for (int num : nums) {
			max = num > max ? num : max;
		}
		return max;
	}

	/**
	 * Scale {@code num}, which is between {@code rawMin} and {@code rawMax} to
	 * be between {@code scaledMin} and {@code scaleMax}.
	 * 
	 * @param num
	 * @param rawMin
	 * @param rawMax
	 * @param scaledMin
	 * @param scaledMax
	 * @return the scaled value
	 * @see http
	 *      ://stackoverflow.com/questions/5294955/how-to-scale-down-a-range-
	 *      of-numbers-with-a-known-min-and-max-value
	 */
	public static Number scale(Number num, Number rawMin, Number rawMax,
			Number scaledMin, Number scaledMax) {
		double x = num.doubleValue();
		double min = rawMin.doubleValue();
		double max = rawMax.doubleValue();
		double a = scaledMin.doubleValue();
		double b = scaledMax.doubleValue();
		Preconditions.checkArgument(x >= min && x <= max,
				"The value of num must be between rawMin and rawMax");
		return (((b - a) * (x - min)) / (max - min)) + a;
	}

}
