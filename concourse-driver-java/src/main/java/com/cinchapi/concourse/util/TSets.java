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
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.Sets;

/**
 * Utilities for {@link Set} objects. This class is mostly designed to offer
 * performance improvements over some of the operations in the Guava
 * {@link Sets} class.
 * 
 * @author Jeff Nelson
 */
public class TSets {

    /**
     * Return a Set that contains only the elements that are in both {@code a}
     * and {@code b}.
     * 
     * @param a
     * @param b
     * @return the intersection of the Sets
     */
    public static <T> Set<T> intersection(Set<T> a, Set<T> b) {
        if(a instanceof SortedSet && b instanceof SortedSet) {
            return sortedIntersection((SortedSet<T>) a, (SortedSet<T>) b);
        }
        else {
            Set<T> intersection = Sets.newLinkedHashSet();
            Set<T> smaller = a.size() <= b.size() ? a : b;
            Set<T> larger = Sets.newHashSet(a.size() > b.size() ? a : b);
            for (T element : smaller) {
                if(larger.contains(element)) {
                    intersection.add(element);
                }
            }
            return intersection;
        }
    }

    /**
     * Return a Set that contains all the elements in both {@code a} and
     * {@code b}.
     * 
     * @param a
     * @param b
     * @return the union of the Sets
     */
    public static <T> Set<T> union(Set<T> a, Set<T> b) {
        Set<T> union = Sets.newLinkedHashSet(a);
        union.addAll(b);
        return union;
    }

    /**
     * Perform an optimized intersection calculation, assuming that both
     * {@code a} and {@code b} are sorted sets.
     * 
     * @param a
     * @param b
     * @return the intersection of the Sets
     */
    @SuppressWarnings("unchecked")
    private static <T> Set<T> sortedIntersection(SortedSet<T> a, SortedSet<T> b) {
        Set<T> intersection = Sets.newLinkedHashSet();
        if(a.isEmpty() || b.isEmpty()) {
            return intersection;
        }
        Iterator<T> ait = a.iterator();
        Iterator<T> bit = b.iterator();
        Comparator<? super T> comp = a.comparator();
        boolean ago = true;
        boolean bgo = true;
        T aelt = null;
        T belt = null;
        while (((ago && ait.hasNext()) || !ago)
                && ((bgo && bit.hasNext()) || !bgo)) {
            aelt = ago ? ait.next() : aelt;
            belt = bgo ? bit.next() : belt;
            int order = comp == null ? ((Comparable<T>) aelt).compareTo(belt)
                    : comp.compare(aelt, belt);
            if(order == 0) {
                intersection.add(aelt);
                ago = true;
                bgo = true;
            }
            else if(order > 0) {
                bgo = true;
                ago = false;
            }
            else {
                ago = true;
                bgo = false;
            }
        }
        return intersection;
    }

}
