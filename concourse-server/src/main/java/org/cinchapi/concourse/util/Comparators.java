/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

/**
 * A collection of commonly used comparator instances. These comparator
 * instances are easy enough to make, but this class provides them so that there
 * is a canonical set, which allows us to avoid creating lots of temporary
 * objects just to do sorting.
 * 
 * @author jnelson
 */
public final class Comparators {

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

}
