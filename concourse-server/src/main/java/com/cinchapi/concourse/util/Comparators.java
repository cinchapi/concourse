/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.util.Comparator;

import com.google.common.collect.Ordering;

/**
 * A collection of commonly used comparator instances. These comparator
 * instances are easy enough to make, but this class provides them so that there
 * is a canonical set, which allows us to avoid creating lots of temporary
 * objects just to do sorting.
 * 
 * @author Jeff Nelson
 */
public final class Comparators {

    /**
     * Return a {@link Comparator} that always returns {@code 0} when two
     * objects are equal, but otherwise returns an arbitrary but consistent
     * ordering for objects during the course of the JVM lifecycle.
     * 
     * @return the comparator
     */
    public static <T> Comparator<T> equalOrArbitrary() {
        return new EqualOrArbitraryComparator<T>();
    }

    /**
     * Return a {@link Comparator} that sorts elements by their natural order if
     * they are {@link Comparable} and sorts them in an arbitrary, but per
     * JVM-lifecycle consistent, order if they are not.
     * 
     * @return the comparator
     */
    public static <T> Comparator<T> naturalOrArbitrary() {
        return new NaturalOrArbitraryComparator<T>();
    }

    /**
     * Perform an arbitrary comparison between {@code o1} and {@code o2}. This
     * method assumes that the two objects are not considered equal.
     * 
     * @param o1
     * @param o2
     * @return the comparison value
     */
    private static <T> int arbitraryCompare(T o1, T o2) {
        return Ordering.arbitrary().compare(o1, o2);
    }

    /**
     * A comparator that sorts strings lexicographically without regards to
     * case.
     */
    public final static Comparator<String> CASE_INSENSITIVE_STRING_COMPARATOR = new Comparator<String>() {

        @Override
        public int compare(String s1, String s2) {
            return s1.compareToIgnoreCase(s2);
        }

    };

    /**
     * A comparator that sorts longs in numerical order.
     */
    public final static Comparator<Long> LONG_COMPARATOR = new Comparator<Long>() {

        @Override
        public int compare(Long o1, Long o2) {
            return Long.compare(o1, o2);
        }

    };

    private Comparators() {/* noop */}

    /**
     * A {@link Comparator} that is similar to {@link Ordering#arbitrary()} in
     * that in will return an arbitrary but consistent ordering of objects
     * (during the duration of the JVM lifecycle), unless they are equal, in
     * which case it returns 0.
     * 
     * @author Jeff Nelson
     */
    private static class EqualOrArbitraryComparator<T> implements Comparator<T> {

        @Override
        public int compare(T o1, T o2) {
            if(o1 == o2 || o1.equals(o2)) {
                return 0;
            }
            else {
                return arbitraryCompare(o1, o2);
            }
        }

    }

/**
     * A {@link Comparator} that uses the functionality of the {@link Ordering
     * @natural()} comparator if the objects are {@link Comparable}. Otherwise,
     * the functionality is similar to the {@link Ordering#arbitrary()}
     * comparator.
     * 
     * 
     * @author Jeff Nelson
     */
    private static class NaturalOrArbitraryComparator<T> implements
            Comparator<T> {

        @SuppressWarnings("unchecked")
        @Override
        public int compare(T o1, T o2) {
            if(o1 instanceof Comparable) {
                return ((Comparable<T>) o1).compareTo(o2);
            }
            else {
                return arbitraryCompare(o1, o2);
            }
        }

    }

}
