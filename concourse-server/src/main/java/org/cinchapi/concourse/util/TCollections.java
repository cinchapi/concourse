/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Collection;

import com.google.common.collect.Iterables;

/**
 * A collection of {@link Collection} related utility functions.
 * 
 * @author jnelson
 */
public final class TCollections {

    /**
     * Return the collection, {@code a} or {@code b}, that has the fewest
     * elements. If both collections have the same number of elements, then the
     * first collection, {@code a} is returned.
     * 
     * @param a
     * @param b
     * @return the collection with the fewest elements
     */
    public static Collection<?> smallerBetween(Collection<?> a, Collection<?> b) {
        return a.size() == b.size() || a.size() < b.size() ? a : b;
    }

    /**
     * Return the collection, {@code a} or {@code b}, that has the most
     * elements. If both collections have the same number of elements, then the
     * second collection, {@code b} is returned.
     * 
     * @param a
     * @param b
     * @return the collection with the most elements
     */
    public static Collection<?> largerBetween(Collection<?> a, Collection<?> b) {
        return a.size() == b.size() || b.size() > a.size() ? b : a;
    }

    /**
     * Return a random element from the {@code collection}.
     * 
     * @param collection
     * @return a random element
     */
    public static <T> T getRandomElement(Collection<T> collection) {
        int size = collection.size();
        return Iterables.get(collection, Math.abs(Random.getInt()) % size);
    }

    /**
     * Return a string that displays the {@code collection} as an order
     * numerical list.
     * 
     * @param collection
     * @return the string output
     */
    public static <T> String toOrderedListString(Iterable<T> collection) {
        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (T item : collection) {
            sb.append(i).append(". ").append(item)
                    .append(System.getProperty("line.separator"));
            ++i;
        }
        return sb.toString();
    }

    private TCollections() {/* noop */}
}
