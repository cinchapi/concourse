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

import java.util.Map;
import java.util.Set;

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

    private MultimapViews() {/* noop */}

}
