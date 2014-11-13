/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Map based utility methods that are not found in the Guava
 * {@link com.google.collect.Maps} utility class.
 * 
 * @author jnelson
 */
public final class TMaps {

    /**
     * Return a map that has all the data in {@code map} in naturally sorted
     * order.
     * 
     * @param map
     * @return the sorted map
     */
    public static <K extends Comparable<K>, V> SortedMap<K, V> asSortedMap(
            Map<K, V> map) {
        if(map instanceof SortedMap) {
            return (SortedMap<K, V>) map;
        }
        else {
            return new TreeMap<K, V>(map);
        }

    }

    private TMaps() {/* noop */}

}
