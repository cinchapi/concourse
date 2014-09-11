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
package org.cinchapi.common.util;

import java.util.AbstractMap;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

/**
 * A thread-safe lock-free alternate implementation of a {@link HashMultimap}.
 * 
 * @author jnelson
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

    @Override
    public Collection<Entry<K, V>> entries() {
        Set<Entry<K, V>> entries = new NonBlockingHashSet<Entry<K, V>>();
        for (K key : keySet()) {
            for (V value : get(key)) {
                entries.add(new AbstractMap.SimpleEntry<K, V>(key, value));
            }
        }
        return entries;
    }

    @Override
    public Set<V> get(K key) {
        if(map.containsKey(key)) {
            return map.get(key);
        }
        else {
            return emptySet;
        }
    }

    @Override
    public boolean isEmpty() {
        return totalSize == 0;
    }

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
            totalSize++;
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public boolean putAll(K key, Iterable<? extends V> values) {
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
        if(values != null) {
            boolean result = values.remove(value);
            totalSize--;
            if(values.isEmpty()) {
                map.remove(values);
            }
            return result;
        }
        else {
            return false;
        }
    }

    @Override
    public Collection<V> removeAll(Object key) {
        Set<V> old = map.remove(key);
        totalSize -= old.size();
        return old;
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
