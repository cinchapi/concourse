/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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

import java.math.BigDecimal;

import com.cinchapi.concourse.Link;
import com.google.common.primitives.UnsignedLongs;

import static com.google.common.base.Preconditions.*;

/**
 * This class contains utility methods that provide interoperability for the
 * various {@link Number} types.
 * 
 * @author Jeff Nelson
 * @author Raghav Babu
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

    /**
     * Checks the instance type of input {@link Number} and returns a
     * corresponding {@link BigDecimal}.
     * 
     * @param number
     * @return {@link BigDecimal}
     */
    public static BigDecimal toBigDecimal(Number number) {
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
        return null;
    }

    /**
     * Checks the instance type of input {@link BigDecimal} and returns a
     * corresponding {@link Number}.
     * 
     * @param {@link BigDecimal}
     * @return {@link Number}
     */
    public static Number toNumber(BigDecimal bigDecimal) {
        return Strings.tryParseNumberStrict(bigDecimal.toString());
    }
}
