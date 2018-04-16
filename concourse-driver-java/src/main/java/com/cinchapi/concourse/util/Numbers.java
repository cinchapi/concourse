/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import static com.google.common.base.Preconditions.*;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.cinchapi.concourse.Link;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedLongs;

/**
 * This class contains utility methods that provide interoperability for the
 * various {@link Number} types.
 * 
 * @author Jeff Nelson
 * @author Raghav Babu
 */
public abstract class Numbers {

    /**
     * The default value for {@link #DIVISION_NUMBER_DECIMAL_PLACES}.
     */
    private static final int DEFAULT_DIVISION_NUMBER_DECIMAL_PLACES = 10;

    /**
     * The number of decimal places to include when
     * {@link #divide(Number, Number) dividing}.
     */
    private static final int DIVISION_NUMBER_DECIMAL_PLACES = Integer
            .parseInt(System.getProperty(
                    "com.cinchapi.concourse.calculation.divisionNumberDecimalPlaces",
                    Integer.toString(DEFAULT_DIVISION_NUMBER_DECIMAL_PLACES)));

    /**
     * Return the sum of two numbers.
     * 
     * @param a the first {@link Number}
     * @param b the second {@link Number}
     * @return the sum of {@code a} and {@code b}.
     */
    public static Number add(Number a, Number b) {
        if(Numbers.isFloatingPoint(a) || Numbers.isFloatingPoint(b)) {
            BigDecimal a0 = Numbers.toBigDecimal(a);
            BigDecimal b0 = Numbers.toBigDecimal(b);
            return a0.add(b0);
        }
        else {
            try {
                return Math.addExact(a.intValue(), b.intValue());
            }
            catch (ArithmeticException e) {
                return Math.addExact(a.longValue(), b.longValue());
            }
        }
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
        Class<?> aClass = a.getClass();
        Class<?> bClass = b.getClass();
        if((aClass == int.class || aClass == Integer.class)
                && (bClass == int.class || bClass == Integer.class)) {
            return Integer.compare(a.intValue(), b.intValue());
        }
        else if((aClass == long.class || aClass == Long.class)
                && (bClass == long.class || bClass == Long.class)) {
            return Long.compare(a.longValue(), b.longValue());
        }
        else if((aClass == float.class || aClass == Float.class)
                && (bClass == float.class || bClass == Float.class)) {
            return Float.compare(a.floatValue(), b.floatValue());
        }
        else if((aClass == double.class || aClass == Double.class)
                && (bClass == double.class || bClass == Double.class)) {
            return Double.compare(a.doubleValue(), b.doubleValue());
        }
        else if((aClass == short.class || aClass == Short.class)
                && (bClass == short.class || bClass == Short.class)) {
            return Short.compare(a.shortValue(), b.shortValue());
        }
        else if((aClass == byte.class || aClass == Byte.class)
                && (bClass == byte.class || bClass == Byte.class)) {
            return Byte.compare(a.byteValue(), b.byteValue());
        }
        else {
            // TODO review
            String fa = aClass == Link.class
                    ? UnsignedLongs.toString(a.longValue()) : a.toString();
            String sb = bClass == Link.class
                    ? UnsignedLongs.toString(b.longValue()) : b.toString();
            BigDecimal first = new BigDecimal(fa);
            BigDecimal second = new BigDecimal(sb);
            return first.compareTo(second);
        }
    }

    /**
     * Return the division of two numbers.
     * 
     * @param a the first {@link Number}
     * @param b the second {@link Number}
     * @return the division result of {@code a} by {@code b}.
     */
    public static Number divide(Number a, Number b) {
        BigDecimal a0 = Numbers.toBigDecimal(a);
        BigDecimal b0 = Numbers.toBigDecimal(b);
        return a0.divide(b0, DIVISION_NUMBER_DECIMAL_PLACES,
                RoundingMode.HALF_UP);
    }

    /**
     * Compute the incremental average from the current {@code running} of the
     * same, given the latest {@code number} and the total {@code count} of
     * items (including the specified {@code number}).
     * 
     * @param running the running average
     * @param number the next number to include in the average
     * @param count the total number of items, including the specified
     *            {@code number} that contribute to the average
     * @return the new incremental average
     */
    public static Number incrementalAverage(Number running, Number number,
            int count) {
        Number sum = Numbers.multiply(running, count - 1);
        sum = Numbers.add(sum, number);
        return Numbers.divide(sum, count);
    }

    /**
     * Return {@code true} if {@code a} is mathematically equal to {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} == {@code b}
     */
    public static boolean areEqual(Number a, Number b) {
        return compare(a, b) == 0;
    }

    /**
     * Return {@code true} if {@code a} is mathematically equal to {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} == {@code b}
     * @deprecated Deprecated in version 0.7. Use
     *             {@link #areEqual(Number, Number)} instead.
     */
    @Deprecated
    public static boolean isEqualTo(Number a, Number b) {
        return areEqual(a, b);
    }

    /**
     * Perform a cast safe equality check for two objects.
     * <p>
     * If both objects are instances of the {@link Number} class, this method
     * will behave the same was as {@link #isEqualTo(Number, Number)}.
     * Otherwise, this method returns {@code false}.
     * </p>
     * 
     * @param a the first, possibly {@link Number numeric}, object
     * @param b the second, possibly {@link Number numeric}, object
     * @return {@code true} if both objects are numbers and are mathematically
     *         equal
     */
    public static boolean areEqualCastSafe(Object a, Object b) {
        if(a instanceof Number && b instanceof Number) {
            return isEqualTo((Number) a, (Number) b);
        }
        else {
            return false;
        }
    }

    /**
     * Perform a cast safe equality check for two objects.
     * <p>
     * If both objects are instances of the {@link Number} class, this method
     * will behave the same was as {@link #isEqualTo(Number, Number)}.
     * Otherwise, this method returns {@code false}.
     * </p>
     * 
     * @param a the first, possibly {@link Number numeric}, object
     * @param b the second, possibly {@link Number numeric}, object
     * @return {@code true} if both objects are numbers and are mathematically
     *         equal
     * @deprecated Deprecated in version 0.7. Use
     *             {@link #areEqualCastSafe(Number, Number)} instead.
     */
    @Deprecated
    public static boolean isEqualToCastSafe(Object a, Object b) {
        if(a instanceof Number && b instanceof Number) {
            return areEqual((Number) a, (Number) b);
        }
        else {
            return false;
        }
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
     * Return {@code true} if the {@code number} is a floating point type.
     * 
     * @param number the {@link Number} to check
     * @return {@code true} if the {@link Number} is floating point
     */
    public static boolean isFloatingPoint(Number number) {
        return number instanceof Float || number instanceof Double
                || number instanceof BigDecimal;
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
        return compare(a, b) >= 0;
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
     * Return {@code true} if {@code a} is mathematically less than or equal to
     * {@code b}.
     * 
     * @param a
     * @param b
     * @return {@code true} if {@code a} <= {@code b}
     */
    public static boolean isLessThanOrEqualTo(Number a, Number b) {
        return compare(a, b) <= 0;
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
     * Return the product of two numbers.
     * 
     * @param a the first {@link Number}
     * @param b the second {@link Number}
     * @return the product of {@code a} and {@code b}.
     */
    public static Number multiply(Number a, Number b) {
        if(Numbers.isFloatingPoint(a) || Numbers.isFloatingPoint(b)) {
            BigDecimal a0 = Numbers.toBigDecimal(a);
            BigDecimal b0 = Numbers.toBigDecimal(b);
            return a0.multiply(b0);
        }
        else {
            try {
                return Math.multiplyExact(a.intValue(), b.intValue());
            }
            catch (ArithmeticException e) {
                return Math.multiplyExact(a.longValue(), b.longValue());
            }
        }
    }

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
     * Round the numeric {@code value} to {@code places} without truncating.
     * 
     * @param value the number to round
     * @param places the max number of decimal places
     * @return the rounded value, represented as a double
     */
    public static double round(Number value, int places) {
        Preconditions.checkArgument(places > 0);
        return new BigDecimal(value.doubleValue())
                .setScale(places, RoundingMode.HALF_UP).doubleValue();
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

    /**
     * Checks the instance type of input {@link Number} and returns a
     * corresponding {@link BigDecimal}.
     * 
     * @param number
     * @return {@link BigDecimal}
     */
    public static BigDecimal toBigDecimal(Number number) {
        // TODO check for primitive classes...
        if(number == null) {
            return null;
        }
        if(number instanceof Integer) {
            return new BigDecimal(number.intValue());
        }
        else if(number instanceof Double) {
            return new BigDecimal(number.doubleValue());
        }
        else if(number instanceof Float) {
            return new BigDecimal(number.floatValue());
        }
        else if(number instanceof Long) {
            return new BigDecimal(number.longValue());
        }
        else if(number instanceof Byte) {
            return new BigDecimal(number.byteValue());
        }
        else if(number instanceof Short) {
            return new BigDecimal(number.shortValue());
        }
        else if(number instanceof BigDecimal) {
            return (BigDecimal) number;
        }
        return null;
    }
}
