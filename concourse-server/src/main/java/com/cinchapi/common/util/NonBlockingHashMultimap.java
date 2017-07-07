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
package com.cinchapi.common.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

/**
 * A thread-safe lock-free alternate implementation of a
 * {@link com.google.common.collect.HashMultimap HashMultimap}.
 * <p>
 * This structure differs from the {@link Multimap} interface in some ways. For
 * example, views returned from methods such as {@link #keys()},
 * {@link #entries()}, etc do not "read through" to the underlying collection.
 * That means changes made to those views do not update the Multimap instance
 * and vice-versa.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class NonBlockingHashMultimap<K, V> implements Multimap<K, V> {

    /**
     * Return a new {@link NonBlockingHashMultimap}.
     * 
     * @return the map
     */
    public static <K, V> NonBlockingHashMultimap<K, V> create() {
        return new NonBlockingHashMultimap<K, V>(
                new NonBlockingHashMap<K, Set<V>>());
    }

    /**
     * Construct a new instance.
     * 
     * @param map
     */
    private NonBlockingHashMultimap(NonBlockingHashMap<K, Set<V>> map) {
        this.map = map;

    }

    /**
     * A immutable empty set that does not throw exceptions if a caller attempts
     * a modification.
     */
    private final NoOpEmptySet emptySet = new NoOpEmptySet();

    /**
     * The backing store.
     */
    private final NonBlockingHashMap<K, Set<V>> map;

    /**
     * An accumulation of the number of entries that are in the multimap at a
     * given time.
     */
    private int totalSize;

    @Override
    public Map<K, Collection<V>> asMap() {
        Map<K, Collection<V>> theMap = Maps.newHashMap();
        for (K key : keySet()) {
            theMap.put(key, get(key));
        }
        return theMap;
    }

    @Override
    public void clear() {
        map.clear();
        totalSize = 0;

    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsEntry(Object key, Object value) {
        try {
            return get((K) key).contains(value);
        }
        catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (Set<V> set : map.values()) {
            if(set.contains(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> This method deviates from the original
     * {@link Multimap} interface in that changes to the returned collection
     * WILL NOT update the underlying multimap and vice-versa.
     */
    @Override
    public Collection<Entry<K, V>> entries() {
        Set<Entry<K, V>> entries = new NonBlockingHashSet<Entry<K, V>>();
        for (K key : keySet()) {
            for (V value : get(key)) {
                entries.add(Maps.immutableEntry(key, value));
            }
        }
        return entries;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> This method deviates from the original
     * {@link Multimap} interface in that changes to the returned collection
     * WILL NOT update the underlying multimap and vice-versa.
     */
    @Override
    public Set<V> get(K key) {
        Set<V> values = map.get(key);
        return values != null ? values : emptySet;
    }

    @Override
    public boolean isEmpty() {
        return totalSize == 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> This method deviates from the original
     * {@link Multimap} interface in that changes to the returned coll ection
     * WILL NOT update the underlying multimap and vice-versa.
     * </p>
     */
    @Override
    public Multiset<K> keys() {
        Set<K> keys = keySet();
        HashMultiset<K> multiset = HashMultiset.create(keys.size());
        for (K key : keys) {
            multiset.add(key, get(key).size());
        }
        return multiset;
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public boolean put(K key, V value) {
        Set<V> values = map.get(key);
        if(values == null) {
            values = new NonBlockingHashSet<V>();
            map.put(key, values);
        }
        if(values.add(value)) {
            ++totalSize;
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public boolean putAll(K key, Iterable<? extends V> values) {
        if(!Iterables.isEmpty(values)) {
            Set<V> set = get(key);
            if(set == emptySet) {
                set = new NonBlockingHashSet<V>();
                map.put(key, set);
            }
            int currentSize = set.size();
            Iterators.addAll(set, values.iterator());
            if(set.size() - currentSize > 0) {
                totalSize += (set.size() - currentSize);
                return true;
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    @Override
    public boolean putAll(Multimap<? extends K, ? extends V> multimap) {
        boolean changed = false;
        for (Map.Entry<? extends K, ? extends V> entry : multimap.entries()) {
            changed |= put(entry.getKey(), entry.getValue());
        }
        return changed;
    }

    @Override
    public boolean remove(Object key, Object value) {
        Set<V> values = map.get(key);
        if(values != null && values.remove(value)) {
            totalSize--;
            if(values.isEmpty()) {
                map.remove(key);
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public Collection<V> removeAll(Object key) {
        Set<V> old = map.remove(key);
        if(old != null) {
            totalSize -= old.size();
            return old;
        }
        else {
            return emptySet;
        }
    }

    @Override
    public Collection<V> replaceValues(K key, Iterable<? extends V> values) {
        Collection<V> result = removeAll(key);
        putAll(key, values);
        return result;
    }

    @Override
    public int size() {
        return totalSize;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>NOTE:</strong> This method deviates from the original
     * {@link Multimap} interface in that changes to the returned collection
     * WILL NOT update the underlying multimap and vice-versa.
     */
    @Override
    public Collection<V> values() {
        List<V> values = Lists.newArrayList();
        for (Entry<K, V> entry : entries()) {
            values.add(entry.getValue());
        }
        return values;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof NonBlockingHashMultimap) {
            return map.equals(((NonBlockingHashMultimap<K, V>) obj).map);
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {
        return map.toString();
    }

    /**
     * An empty set that is non-operable. Attempts to modify this set won't
     * throw any exceptions and will just silently be ignored.
     */
    private final class NoOpEmptySet implements Set<V> {
        @Override
        public boolean add(V e) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            return false;
        }

        @Override
        public void clear() {}

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Iterator<V> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Object[] toArray() {
            return new Object[] {};
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return a;
        }

    }

}
