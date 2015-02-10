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
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A collection of utility methods that abstracts adding and removing elements
 * to a map of keys to a collection of values (e.g. checks will be performed to
 * see if a key has an associated set before adding and empty sets will be
 * cleared on value removal, etc). This class is meant to be used
 * over Guava multimaps for performance gains.
 * 
 * @author jnelson
 */
public final class MultimapViews {

    /**
     * Return the set of values mapped from {@code key} in the {@code map} or an
     * empty set if no values exist.
     * 
     * @param map
     * @param key
     * @return the set of values
     */
    public static <K, V> Set<V> get(Map<K, Set<V>> map, Object key) {
        Set<V> values = map.get(key);
        return values != null ? values : Sets.<V> newHashSetWithExpectedSize(0);
    }

    /**
     * Associates the {@code value} with the {@code key} in the {@code map} if
     * it does not already exist.
     * 
     * @param map
     * @param key
     * @param value
     * @return {@code true} if the new associated is added, {@code false}
     *         otherwise
     */
    public static <K extends Comparable<K>, V> boolean put(Map<K, Set<V>> map,
            K key, V value) {
        Set<V> set = map.get(key);
        if(set == null) {
            set = Sets.newHashSet();
            map.put(key, set);
        }
        return set.add(value);
    }

    /**
     * Remove the {@code value} from the collection mapped from {@code key} in
     * the {@code map}. If the {@code value} is the only one associated with the
     * {@code key} in the map then clear the {@code key}.
     * 
     * @param map
     * @param key
     * @param value
     * @return {@code true} if the value existed and is removed, {@code false}
     *         otherwise
     */
    public static <K extends Comparable<K>, V> boolean remove(
            Map<K, Set<V>> map, K key, V value) {
        Set<V> set = map.get(key);
        if(set != null && set.size() == 1) {
            map.remove(key);
        }
        return set.remove(value);
    }

    /**
     * Flatten the values in the {@code map} and return a collection that
     * contains them all.
     * 
     * @param map
     * @return the values in the map
     */
    public static <K, V> Collection<V> values(Map<K, Set<V>> map) {
        List<V> values = Lists.newArrayList();
        for (Set<V> set : map.values()) {
            for (V value : set) {
                values.add(value);
            }
        }
        return values;
    }

    private MultimapViews() {/* noop */}

}
