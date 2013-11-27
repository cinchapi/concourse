/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
import java.util.TreeMap;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.cinchapi.concourse.annotate.Experimental;

import com.google.common.base.Function;

/**
 * AutoMap with sorting and navigation. Create using
 * {@link AutoMap#newAutoSkipListMap(Function, Function)}.
 * 
 * @author jnelson
 */
@Experimental
public class AutoSkipListMap<K extends Comparable<K>, V> extends AutoMap<K, V> implements
        NavigableMap<K, V> {

    /**
     * Construct a new instance.
     * 
     * @param loader
     * @param cleaner
     */
    protected AutoSkipListMap(Function<K, V> loader,
            Function<V, Boolean> cleaner) {
        super(new ConcurrentSkipListMap<K, V>(), loader, cleaner);
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        return ((TreeMap<K, V>) backingStore).ceilingEntry(key);
    }

    @Override
    public K ceilingKey(K key) {
        return ((TreeMap<K, V>) backingStore).ceilingKey(key);
    }

    @Override
    public Comparator<? super K> comparator() {
        return ((TreeMap<K, V>) backingStore).comparator();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return ((TreeMap<K, V>) backingStore).descendingKeySet();
    }

    @Override
    public NavigableMap<K, V> descendingMap() {
        return ((TreeMap<K, V>) backingStore).descendingMap();
    }

    @Override
    public Entry<K, V> firstEntry() {
        return ((TreeMap<K, V>) backingStore).firstEntry();
    }

    @Override
    public K firstKey() {
        return ((TreeMap<K, V>) backingStore).firstKey();
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        return ((TreeMap<K, V>) backingStore).floorEntry(key);
    }

    @Override
    public K floorKey(K key) {
        return ((TreeMap<K, V>) backingStore).floorKey(key);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return ((TreeMap<K, V>) backingStore).headMap(toKey);
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return ((TreeMap<K, V>) backingStore).headMap(toKey, inclusive);
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        return ((TreeMap<K, V>) backingStore).higherEntry(key);
    }

    @Override
    public K higherKey(K key) {
        return ((TreeMap<K, V>) backingStore).higherKey(key);
    }

    @Override
    public Entry<K, V> lastEntry() {
        return ((TreeMap<K, V>) backingStore).lastEntry();
    }

    @Override
    public K lastKey() {
        return ((TreeMap<K, V>) backingStore).lastKey();
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        return ((TreeMap<K, V>) backingStore).lowerEntry(key);
    }

    @Override
    public K lowerKey(K key) {
        return ((TreeMap<K, V>) backingStore).lowerKey(key);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return ((TreeMap<K, V>) backingStore).navigableKeySet();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        return ((TreeMap<K, V>) backingStore).pollFirstEntry();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        return ((TreeMap<K, V>) backingStore).pollLastEntry();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,
            boolean toInclusive) {
        return ((TreeMap<K, V>) backingStore).subMap(fromKey, fromInclusive,
                toKey, toInclusive);
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        return ((TreeMap<K, V>) backingStore).subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return ((TreeMap<K, V>) backingStore).tailMap(fromKey);
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return ((TreeMap<K, V>) backingStore).tailMap(fromKey, inclusive);
    }

}
