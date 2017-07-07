/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Map based utility methods that are not found in the Guava
 * {@link com.google.collect.Maps} utility class.
 * 
 * @author Jeff Nelson
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

    /**
     * Return <em>mutable</em>, insertion-ordered {@link LinkedHashMap} instance
     * with enough room to fit {@code capacity} items.
     * 
     * <p>
     * Use this method over
     * {@link com.google.common.collect.Maps#newLinkedHashMap()} when the size
     * of the map is known in advance and we are being careful to not oversize
     * collections.
     * </p>
     * 
     * @param capacity the initial capacity
     * @return a new, empty {@link LinkedHashMap}
     */
    public static <K, V> Map<K, V> newLinkedHashMapWithCapacity(int capacity) {
        return new LinkedHashMap<K, V>(capacity);
    }

    /**
     * The same as
     * {@link java.util.concurrent.ConcurrentMap#putIfAbsent(Object, Object))}
     * but for non concurrent maps. This method is purely syntactic sugar.
     * 
     * @param map the map to modify
     * @param key the key to lookup
     * @param value the value to associate with {@code key} if no other
     *            currently exists
     * @return the value that is already associated with {@code key} if it
     *         exists, otherwise {@code value}
     */
    public static <K, V> V putIfAbsent(Map<K, V> map, K key, V value) {
        V stored = map.get(key);
        if(stored == null) {
            stored = value;
            map.put(key, value);
        }
        return stored;
    }

    /**
     * Use the {@code supplier} to provide a value to associate with the
     * {@code key} in the {@code map} if there isn't currently an associated
     * value.
     * 
     * @param map the map to modify
     * @param key the key to lookup and/or associate
     * @param supplier the {@link Supplier} that lazily produces the value to
     *            associate if no other value currently exists
     * @return the value that is already associated with {@code key} if it
     *         exists, otherwise the supplied value
     */
    public static <K, V> V supplyIfAbsent(Map<K, V> map, K key,
            Supplier<V> supplier) {
        V stored = map.get(key);
        if(stored == null) {
            stored = supplier.get();
            map.put(key, stored);
        }
        return stored;
    }

    private TMaps() {/* noop */}

}
