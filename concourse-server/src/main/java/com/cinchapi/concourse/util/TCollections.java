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

import java.util.Collection;

import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Iterables;

/**
 * A collection of {@link Collection} related utility functions.
 * 
 * @author Jeff Nelson
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
