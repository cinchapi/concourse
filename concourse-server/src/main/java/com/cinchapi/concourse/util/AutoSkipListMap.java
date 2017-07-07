/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.cinchapi.concourse.annotate.Experimental;
import com.google.common.base.Function;

/**
 * AutoMap with sorting and navigation. Create using
 * {@link AutoMap#newAutoSkipListMap(Function, Function)}.
 * 
 * @author Jeff Nelson
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
        return ((ConcurrentSkipListMap<K, V>) backingStore).ceilingEntry(key);
    }

    @Override
    public K ceilingKey(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).ceilingKey(key);
    }

    @Override
    public Comparator<? super K> comparator() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).comparator();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).descendingKeySet();
    }

    @Override
    public NavigableMap<K, V> descendingMap() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).descendingMap();
    }

    @Override
    public Entry<K, V> firstEntry() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).firstEntry();
    }

    @Override
    public K firstKey() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).firstKey();
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).floorEntry(key);
    }

    @Override
    public K floorKey(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).floorKey(key);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).headMap(toKey);
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).headMap(toKey, inclusive);
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).higherEntry(key);
    }

    @Override
    public K higherKey(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).higherKey(key);
    }

    @Override
    public Entry<K, V> lastEntry() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).lastEntry();
    }

    @Override
    public K lastKey() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).lastKey();
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).lowerEntry(key);
    }

    @Override
    public K lowerKey(K key) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).lowerKey(key);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).navigableKeySet();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).pollFirstEntry();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        return ((ConcurrentSkipListMap<K, V>) backingStore).pollLastEntry();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,
            boolean toInclusive) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).subMap(fromKey, fromInclusive,
                toKey, toInclusive);
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).tailMap(fromKey);
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return ((ConcurrentSkipListMap<K, V>) backingStore).tailMap(fromKey, inclusive);
    }

}
