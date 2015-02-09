/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
 * @author jnelson
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
        //TODO: look into CON-245
//        if(a instanceof SortedSet && b instanceof SortedSet) {
//            return sortedIntersection((SortedSet<T>) a, (SortedSet<T>) b);
//        }
//        else {
            Set<T> intersection = Sets.newLinkedHashSet();
            Set<T> smaller = a.size() <= b.size() ? a : b;
            Set<T> larger = Sets.newHashSet(a.size() > b.size() ? a : b);
            for (T element : smaller) {
                if(larger.contains(element)) {
                    intersection.add(element);
                }
            }
            return intersection;
//        }
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
        Iterator<T> ait = a.iterator();
        Iterator<T> bit = b.iterator();
        Comparator<? super T> comp = a.comparator();
        boolean ago = true;
        boolean bgo = true;
        T aelt = null;
        T belt = null;
        while (ait.hasNext() && bit.hasNext()) {
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
