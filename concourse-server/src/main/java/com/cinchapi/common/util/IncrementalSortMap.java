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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * An {@link IncrementalSortMap} is a {@link ConcurrentNavigableMap} that is
 * optimized for adding elements in constant time while enjoying the benefits of
 * a typical sorted collection.
 * <p>
 * Internally, this map initially stores elements in several hashmaps. The
 * elements are incrementally sorted only when a call is made to a method that
 * requires the elements to be in sorted order.
 * </p>
 * <p>
 * The benefit to this approach is that writes to the map are very fast (e.g.
 * constant time). Retrievals <em>may</em> occur in constant time (if the key
 * has not yet been sorted), but never perform any worse than retrieval times
 * for typical sorted collections. With that said, we do make tradeoffs in cases
 * where there is a requirement to get a sorted view of the map or perform an
 * operation in the {@link java.util.NavigableMap NavigableMap} interface. In
 * those cases, all the unsorted elements in the map, must be sorted first and
 * then the traversal must happen.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class IncrementalSortMap<K, V> implements ConcurrentNavigableMap<K, V> {

    /**
     * Return a new {@link IncrementalSortMap} that sorts elements based on
     * their natural order.
     * 
     * @return the IncrementalSortMap
     */
    public static <K, V> IncrementalSortMap<K, V> create() {
        return new IncrementalSortMap<K, V>(null);
    }

    /**
     * Return an {@link IncrementalSortMap} that sorts elements based on the
     * rules of the specified {@code comparator}.
     * 
     * @param comparator
     * @return the IncrementalSortMap
     */
    public static <K, V> IncrementalSortMap<K, V> create(
            Comparator<? super K> comparator) {
        return new IncrementalSortMap<K, V>(comparator);
    }

    /**
     * The number of internal segments where data is initially written. More
     * segments also us to have higher throughput, but too many can lead to way
     * too much overhead.
     */
    private final int concurrencyLevel = 16; // TODO maybe make this
                                             // configurable?

    /**
     * The locks that are used to control concurrent access to each segment.
     */
    private final StampedLock[] segmentLocks;

    /**
     * The segments where data is initially written. Each key is assigned to a
     * unique segment, and written there before eventually being placed in the
     * {@link #sorted} map.
     */
    private final HashMap<K, V>[] segments;

    /**
     * The sorted collection of data which is incrementally populated from the
     * unsorted {@link #segments}.
     */
    private final ConcurrentSkipListMap<K, V> sorted;

    /**
     * Construct a new instance.
     * 
     * @param comparator
     */
    @SuppressWarnings("unchecked")
    private IncrementalSortMap(Comparator<? super K> comparator) {
        this.sorted = comparator == null ? new ConcurrentSkipListMap<K, V>()
                : new ConcurrentSkipListMap<K, V>(comparator);
        this.segments = new HashMap[concurrencyLevel];
        this.segmentLocks = new StampedLock[concurrencyLevel];
        for (int i = 0; i < concurrencyLevel; i++) {
            segments[i] = Maps.<K, V> newHashMap();
            segmentLocks[i] = new StampedLock();
        }

    }

    @Override
    public java.util.Map.Entry<K, V> ceilingEntry(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.ceilingEntry(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public K ceilingKey(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.ceilingKey(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public void clear() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sorted.clear();
            for (HashMap<K, V> segment : segments) {
                segment.clear();
            }
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public Comparator<? super K> comparator() {
        return sorted.comparator();
    }

    @Override
    public boolean containsKey(Object key) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].readLock();
        try {
            return segments[seg].containsKey(key) || sorted.containsKey(key);
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        long[] stamps = grabAllSegmentReadLocks();
        try {
            for (HashMap<K, V> segment : segments) {
                if(segment.containsValue(value)) {
                    return true;
                }
            }
            return sorted.containsValue(value);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.descendingKeySet();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.descendingMap();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.entrySet();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof IncrementalSortMap) {
            IncrementalSortMap other = (IncrementalSortMap) obj;
            long[] stamps = grabAllSegmentReadLocks();
            long[] otherStamps = other.grabAllSegmentReadLocks();
            try {
                sort();
                other.sort();
                return sorted.equals(other.sorted);
            }
            finally {
                releaseSegmentLocks(stamps);
                other.releaseSegmentLocks(otherStamps);
            }
        }
        else {
            return false;
        }
    }

    @Override
    public java.util.Map.Entry<K, V> firstEntry() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.firstEntry();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public K firstKey() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.firstKey();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public java.util.Map.Entry<K, V> floorEntry(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.floorEntry(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public K floorKey(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.floorKey(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public V get(Object key) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].readLock();
        try {
            V val = segments[seg].get(key);
            if(val == null) {
                val = sorted.get(key);
            }
            return val;
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public int hashCode() {
        long[] stamps = grabAllSegmentReadLocks();
        try {
            sort();
            return sorted.hashCode();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.headMap(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.headMap(toKey, inclusive);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public java.util.Map.Entry<K, V> higherEntry(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.higherEntry(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public K higherKey(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.higherKey(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public boolean isEmpty() {
        long[] stamps = grabAllSegmentReadLocks();
        try {
            for (HashMap<K, V> segment : segments) {
                if(!segment.isEmpty()) {
                    return false;
                }
            }
            return sorted.isEmpty();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public NavigableSet<K> keySet() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.keySet();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public java.util.Map.Entry<K, V> lastEntry() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.lastEntry();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public K lastKey() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.lastKey();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public java.util.Map.Entry<K, V> lowerEntry(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.lowerEntry(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public K lowerKey(K key) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.lowerKey(key);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.navigableKeySet();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public java.util.Map.Entry<K, V> pollFirstEntry() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.pollFirstEntry();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public java.util.Map.Entry<K, V> pollLastEntry() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.pollLastEntry();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    /**
     * This implementation departs from the normal contract specified in the
     * {@link Map} implementation because this method ALWAYS returns
     * {@code null}.
     * 
     * @param key
     * @param value
     * @return {@code null}
     */
    @Override
    @Nullable
    public V put(K key, V value) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].writeLock();
        try {
            segments[seg].put(key, value);
            return null; // always return null because we don't want to look in
                         // the sorted map to get the old value for the key
                         // because that would unnecessarily slow down the write
                         // process.
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            if(m instanceof SortedMap) {
                sorted.putAll(m);
            }
            else {
                for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
                    int seg = getSegment(entry.getKey());
                    segments[seg].put(entry.getKey(), entry.getValue());
                }
            }
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].writeLock();
        try {
            V v = segments[seg].get(key);
            v = v == null ? sorted.get(key) : v;
            if(!value.equals(v)) {
                segments[seg].put(key, value);
            }
            return v;
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public V remove(Object key) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].writeLock();
        try {
            V a = segments[seg].remove(key);
            V b = sorted.remove(key);
            try {
                return MoreObjects.firstNonNull(a, b);
            }
            catch (NullPointerException e) {
                return null;
            }
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].writeLock();
        try {
            V v = segments[seg].get(key);
            v = v == null ? sorted.get(key) : v;
            if(value != null && value.equals(v)) {
                segments[seg].remove(key);
                sorted.remove(key);
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public V replace(K key, V value) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].writeLock();
        try {
            V v = segments[seg].get(key);
            v = v == null ? sorted.get(key) : v;
            if(v != null) {
                segments[seg].put(key, value);
            }
            return v;
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        int seg = getSegment(key);
        long stamp = segmentLocks[seg].writeLock();
        try {
            V v = segments[seg].get(key);
            v = v == null ? sorted.get(key) : v;
            if(oldValue.equals(v)) {
                segments[seg].put(key, newValue);
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            segmentLocks[seg].unlock(stamp);
        }
    }

    @Override
    public int size() {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort(); // this is necessary to de-dupe keys
            return sorted.size();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey,
            boolean fromInclusive, K toKey, boolean toInclusive) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.subMap(fromKey, fromInclusive, toKey, toInclusive);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.subMap(fromKey, toKey);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.tailMap(fromKey);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        long[] stamps = grabAllSegmentWriteLocks();
        try {
            sort();
            return sorted.tailMap(fromKey, inclusive);
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public String toString() {
        long[] stamps = grabAllSegmentReadLocks();
        try {
            sort();
            return sorted.toString();
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    @Override
    public Collection<V> values() {
        long[] stamps = grabAllSegmentReadLocks();
        try {
            List<V> values = Lists.newArrayList();
            values.addAll(sorted.values());
            for (HashMap<K, V> segment : segments) {
                values.addAll(segment.values());
            }
            return values;
        }
        finally {
            releaseSegmentLocks(stamps);
        }
    }

    /**
     * Return the segment where {@code key} should be initially written.
     * 
     * @param key
     * @return the segment for {@code key}
     */
    private int getSegment(Object key) {
        return Math.abs(key.hashCode() % 16);
    }

    /**
     * Method that is called by {@link #grabAllSegmentReadLock()} and
     * {@link #grabAllSegmentWriteLocks()} to do the work of grabbing the
     * appropriate lock.
     * 
     * @param read
     * @return the stamps that represent all the locks that were grabbed.
     */
    private long[] grabAllSegmentLocks(boolean read) {
        long[] stamps = new long[segmentLocks.length];
        for (int i = 0; i < stamps.length; i++) {
            stamps[i] = read ? segmentLocks[i].readLock() : segmentLocks[i]
                    .writeLock();
        }
        return stamps;
    }

    /**
     * Grab the read locks for every segment
     * 
     * @return the stamps for the read locks
     */
    private long[] grabAllSegmentReadLocks() {
        return grabAllSegmentLocks(true);
    }

    /**
     * Grab the write lock for every segment.
     * 
     * @return the stamps for the write locks
     */
    private long[] grabAllSegmentWriteLocks() {
        return grabAllSegmentLocks(false);
    }

    /**
     * Release all the segment locks represented by the collection of
     * {@code stamps}.
     * 
     * @param stamps
     */
    private void releaseSegmentLocks(long[] stamps) {
        for (int i = 0; i < stamps.length; i++) {
            if(stamps[i] > 0) {
                segmentLocks[i].unlock(stamps[i]);
            }
        }
    }

    /**
     * Force an incremental sort. Take all the {@link #unsorted} items and place
     * them into the {@link #sorted} collection in the correct positions. This
     * method should only be called when the client is attempting to perform
     * some action in the {@link SortedMap} interface.
     */
    private void sort() {
        for (HashMap<K, V> segment : segments) {
            sorted.putAll(segment);
            segment.clear();
        }
    }

}
