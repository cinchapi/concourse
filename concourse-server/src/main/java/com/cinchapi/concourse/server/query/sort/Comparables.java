/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.sort;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Utility methods for {@link Comparable} objects.
 *
 * @author Jeff Nelson
 */
public final class Comparables {

    /**
     * Compares analogous elements of two {@link Iterable}s. The value of the
     * comparison is equal to the first non-zero
     * comparison value of the component parts.
     * <p>
     * If all elements in both iterables are equal, this comparator returns 0.
     * </p>
     * <p>
     * If the intersection of the two iterables is equal to the size of only one
     * of
     * the iterables, this comparator returns a value that considers the larger
     * iterable to be "smaller"
     * </p>
     * 
     * @param o1
     * @param o2
     * @return the value of the comparison (in accordance with the
     *         {@link Comparable} interface)
     */
    public static <T extends Comparable<T>> int compare(Iterable<T> o1,
            Iterable<T> o2) {
        Comparator<Iterable<T>> comparator = ($1, $2) -> {
            Iterator<T> it1 = $1.iterator();
            Iterator<T> it2 = $2.iterator();
            int c = 0;
            while (it1.hasNext() && it2.hasNext()) {
                T v1 = it1.next();
                T v2 = it2.next();
                c = v1.compareTo(v2);
                if(c != 0) {
                    break;
                }
            }
            if(c == 0 && it1.hasNext()) {
                c = -1;
            }
            else if(c == 0 && it2.hasNext()) {
                c = 1;
            }
            return c;
        };
        return Comparators.<Iterable<T>> nullSafe(comparator).compare(o1, o2);

    }

    /**
     * Compare two objects in a manner that accounts for the possibility that
     * one or both values may be {@code null}, in which case the non-null value
     * is considered "smaller" (so that {@code nulls} sink to the bottom).
     * 
     * @param o1
     * @param o2
     * @return the value of the comparison (in accordance with the
     *         {@link Comparable} interface)
     */
    public static <T extends Comparable<T>> int nullSafeCompare(T o1, T o2) {
        return Comparators.<T> nullSafe(($1, $2) -> $1.compareTo($2))
                .compare(o1, o2);
    }

    private Comparables() {/* no-init */}

}
