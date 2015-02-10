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
package org.cinchapi.common.util;

import java.util.Comparator;

/**
 * Utility functions for {@link Range ranges}.
 * 
 * @author jnelson
 */
public final class Ranges {

    /**
     * Return a {@link Comparator} that sorts ranges based solely on their left
     * values.
     * 
     * @return the left value comparator
     */
    public static <T extends Comparable<T>> Comparator<Range<T>> leftValueComparator() {
        return new Comparator<Range<T>>() {

            @Override
            public int compare(Range<T> o1, Range<T> o2) {
                return o1.compareToLeft(o2);
            }

        };

    }

    /**
     * Return a {@link Comparator} that sorts ranges based solely on their right
     * values.
     * 
     * @return the right value comparator
     */
    public static <T extends Comparable<T>> Comparator<Range<T>> rightValueComparator() {
        return new Comparator<Range<T>>() {

            @Override
            public int compare(Range<T> o1, Range<T> o2) {
                return o1.compareToRight(o2);
            }

        };
    }

    private Ranges() {/* noop */}

}
