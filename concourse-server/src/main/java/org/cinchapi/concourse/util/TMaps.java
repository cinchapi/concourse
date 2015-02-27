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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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

    /**
     * Return a set that contains all of the keys from the {@code entrySet}.
     * 
     * @param entrySet
     * @return the keySet
     */
    public static <K, V> Set<K> extractKeysFromEntrySet(
            Collection<Entry<K, V>> entrySet) {
        Set<K> keys = entrySet instanceof SortedSet ? new TreeSet<K>(
                Comparators.<K> naturalOrArbitrary()) : Sets.<K> newHashSet();
        for (Entry<K, V> entry : entrySet) {
            keys.add(entry.getKey());
        }
        return keys;
    }

    /**
     * Return a collection that contains all of the values from the
     * {@code entrySet}/
     * 
     * @param entrySet
     * @return the values collection
     */
    public static <K, V> Collection<V> extractValuesFromEntrySet(
            Collection<Entry<K, V>> entrySet) {
        List<V> values = Lists.newArrayList();
        for (Entry<K, V> entry : entrySet) {
            values.add(entry.getValue());
        }
        return values;
    }

    /**
     * Return a map that contains all the entries in the {@code entrySet}.
     * 
     * @param entrySet
     * @return the populated map
     */
    public static <K, V> Map<K, V> fromEntrySet(Collection<Entry<K, V>> entrySet) {
        // TODO: Find a better way to do this. Perhaps use reflection to place
        // the entires directly in the map...
        Map<K, V> map = entrySet instanceof SortedSet ? new TreeMap<K, V>(
                Comparators.<K> naturalOrArbitrary()) : Maps
                .<K, V> newHashMap();
        for (Entry<K, V> entry : entrySet) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    private TMaps() {/* noop */}

}
