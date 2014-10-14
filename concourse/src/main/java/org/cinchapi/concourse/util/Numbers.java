/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.*;

/**
 * This class contains utility methods that provide interoperability for the
 * various {@link Number} types.
 * 
 * @author jnelson
 */
public abstract class Numbers {

    /**
     * Return numerator/denominator as a percent.
     * 
     * @param numerator
     * @param denominator
     * @return the percent
     */
    public static double percent(Number numerator, Number denominator) {
        return numerator.doubleValue() * 100.0 / denominator.doubleValue();
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
        if((a.getClass() == Integer.class || a.getClass() == int.class)
                && (b.getClass() == Integer.class || b.getClass() == int.class)) {
            return Integer.compare(a.intValue(), b.intValue());
        }
        else if((a.getClass() == Long.class || a.getClass() == long.class)
                && (b.getClass() == Long.class || b.getClass() == long.class)) {
            return Long.compare(a.longValue(), b.longValue());
        }
        else if((a.getClass() == Float.class || a.getClass() == float.class)
                && (b.getClass() == Float.class || b.getClass() == float.class)) {
            return Float.compare(a.floatValue(), b.floatValue());
        }
        else if((a.getClass() == Double.class || a.getClass() == double.class)
                && (b.getClass() == Double.class || b.getClass() == double.class)) {
            return Double.compare(a.doubleValue(), b.doubleValue());
        }
        else if((a.getClass() == Short.class || a.getClass() == short.class)
                && (b.getClass() == Short.class || b.getClass() == short.class)) {
            return Short.compare(a.shortValue(), b.shortValue());
        }
        else if((a.getClass() == Byte.class || a.getClass() == byte.class)
                && (b.getClass() == Byte.class || b.getClass() == byte.class)) {
            return Long.compare(a.byteValue(), b.byteValue());
        }
        else {
            // TODO review
            BigDecimal first = new BigDecimal(a.toString());
            BigDecimal second = new BigDecimal(b.toString());
            return first.compareTo(second);
        }
    }

    /**
     * Return {@code true} if {@code a} is mathematically greater than {@code b}
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} > {@code b}
     */
    public static boolean isGreaterThan(Number a, Number b) {
        return compare(a, b) > 0;
    }

    /**
     * Return {@code true} if {@code a} is mathematically greater than or equal
     * to {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} >= {@code b}
     */
    public static boolean isGreaterThanOrEqualTo(Number a, Number b) {
        return isGreaterThan(a, b) || isEqualTo(a, b);
    }

    /**
     * Return {@code true} if {@code a} is mathematically less than or equal to
     * {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} <= {@code b}
     */
    public static boolean isLessThanOrEqualTo(Number a, Number b) {
        return isLessThan(a, b) || isEqualTo(a, b);
    }

    /**
     * Return {@code true} if {@code a} is mathematically less than {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} < {@code b}
     */
    public static boolean isLessThan(Number a, Number b) {
        return compare(a, b) < 0;
    }

    /**
     * Return {@code true} if {@code a} is mathematically equal to {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} == {@code b}
     */
    public static boolean isEqualTo(Number a, Number b) {
        return compare(a, b) == 0;
    }

    /**
     * Return {@code true} if {@code number} is evenly divisible by two.
     * 
     * @param number
     * @return {@code true} if {@code number} is even.
     */
    public static boolean isEven(Number number) {
        return number.intValue() % 2 == 0;
    }

    /**
     * Return {@code true} if {@code number} is not evenly divisible by two.
     * 
     * @param number
     * @return {@code true} if {@code number} is odd.
     */
    public static boolean isOdd(Number number) {
        return !isEven(number);
    }

    /**
     * Return the max from a list of {@code numbers}.
     * 
     * @param numbers
     * @return the largest number
     */
    public static Number max(Number... numbers) {
        Number max = numbers[0];
        for (Number number : numbers) {
            max = isGreaterThan(max, number) ? max : number;
        }
        return max;
    }

    /**
     * Return the min from a list of {@code numbers}.
     * 
     * @param numbers
     * @return the smallest number
     */
    public static Number min(Number... numbers) {
        Number min = numbers[0];
        for (Number number : numbers) {
            min = isLessThan(min, number) ? min : number;
        }
        return min;
    }

    /**
     * Scale {@code number}, which is between {@code rawMin} and {@code rawMax}
     * to a value between {@code scaledMin} and {@code scaleMax}.
     * 
     * @param number
     * @param rawMin
     * @param rawMax
     * @param scaledMin
     * @param scaledMax
     * @return the scaled value
     */
    public static Number scale(Number number, Number rawMin, Number rawMax,
            Number scaledMin, Number scaledMax) {
        checkArgument(isGreaterThanOrEqualTo(number, rawMin)
                && isLessThanOrEqualTo(number, rawMax));

        double x = number.doubleValue();
        double min = rawMin.doubleValue();
        double max = rawMax.doubleValue();
        double a = scaledMin.doubleValue();
        double b = scaledMax.doubleValue();
        return (((b - a) * (x - min)) / (max - min)) + a;
    }
}
