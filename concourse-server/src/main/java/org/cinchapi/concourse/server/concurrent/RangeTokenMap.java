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
package org.cinchapi.concourse.server.concurrent;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.common.util.Range;
import org.cinchapi.common.util.Ranges;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.MultimapViews;
import org.cinchapi.concourse.util.TMaps;
import org.cinchapi.concourse.util.TSets;
import org.cinchapi.vendor.jsr166e.ConcurrentHashMapV8;
import org.cinchapi.vendor.jsr166e.StampedLock;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@link RangeTokenMap} is a drop-in replacement for any (possibly)
 * concurrent mapping from a {@link RangeToken} to a value. This collection is
 * preferred in cases where it is beneficial to index on the key of each range
 * token, so they are grouped together and {@link #filter(Text) filterable} for
 * more efficient analysis.
 * 
 * @author jnelson
 */
@ThreadSafe
public class RangeTokenMap<V> implements ConcurrentMap<RangeToken, V> {

    /**
     * Return a new {@link RangeTokenMap}.
     * 
     * @return the RangeTokenMap
     */
    public static <V> RangeTokenMap<V> create() {
        return new RangeTokenMap<V>();
    }

    /**
     * Check the {@code map} for entries that satisfy {@code operator} in
     * relation to {@code value}.
     * 
     * @param operator
     * @param value
     * @param map
     * @return {@code true} if there are any entries that match the filter
     */
    private static <V> boolean checkShardComponent(Operator operator,
            Value value, TreeMap<Range<Value>, Set<Entry<RangeToken, V>>> map) {
        Range<Value> point = Range.point(value);
        switch (operator) {
        case GREATER_THAN:
            return map.tailMap(point, false).values().isEmpty();
        case GREATER_THAN_OR_EQUALS:
            return map.tailMap(point, true).values().isEmpty();
        case LESS_THAN:
            return map.headMap(point, false).values().isEmpty();
        case LESS_THAN_OR_EQUALS:
            return map.headMap(point, true).values().isEmpty();
        case NOT_EQUALS:
            return map
                    .subMap(Range.point(Value.NEGATIVE_INFINITY), false, point,
                            false).values().isEmpty()
                    && map.subMap(point, false,
                            Range.point(Value.POSITIVE_INFINITY), false)
                            .values().isEmpty();
        case EQUALS:
            return map.subMap(point, true, point, true).values().isEmpty();
        default:
            throw new IllegalArgumentException();
        }
    }

    /**
     * Filter the {@code map} for entries that satisfy {@code operator} in
     * relation to {@code value}.
     * 
     * @param operator
     * @param value
     * @param map
     * @return the filtered entries
     */
    private static <V> Set<Entry<RangeToken, V>> filterShardComponent(
            Operator operator, Value value,
            TreeMap<Range<Value>, Set<Entry<RangeToken, V>>> map) {
        Range<Value> point = Range.point(value);
        switch (operator) {
        case GREATER_THAN:
            return Sets.newHashSet(Iterables.concat(map.tailMap(point, false)
                    .values()));
        case GREATER_THAN_OR_EQUALS:
            return Sets.newHashSet(Iterables.concat(map.tailMap(point, true)
                    .values()));
        case LESS_THAN:
            return Sets.newHashSet(Iterables.concat(map.headMap(point, false)
                    .values()));
        case LESS_THAN_OR_EQUALS:
            return Sets.newHashSet(Iterables.concat(map.headMap(point, true)
                    .values()));
        case NOT_EQUALS:
            return Sets.newHashSet(Iterables.concat(Iterables.concat(map
                    .subMap(Range.point(Value.NEGATIVE_INFINITY), false, point,
                            false).values()), Iterables.concat(map.subMap(
                    point, false, Range.point(Value.POSITIVE_INFINITY), false)
                    .values())));
        case EQUALS:
            return Sets.newHashSet(Iterables.concat(map.subMap(point, true,
                    point, true).values()));
        default:
            throw new IllegalArgumentException();
        }
    }

    /**
     * A mapping from range token keys to the appropriate shard, which mimics a
     * mapping from RangeToken to V.
     * 
     */
    private final ConcurrentMap<Text, Shard> data = new ConcurrentHashMapV8<Text, Shard>();

    /**
     * An empty map that is returned in the {@link #safeGet()} method so that
     * callers can read through to it without having to perform a {@code null}
     * check.
     */
    private Shard safeEmptyMap = new Shard();

    @Override
    public void clear() {
        data.clear();
    }

    /**
     * Filter this map and return one that only has range tokens with the
     * specified {@code key} and a start value that satisfies the
     * {@code startOperator} in relation to the specified {@code start} value
     * joined by an end value that satisfies the {@code endOperator} in relation
     * to the specified {@code end} value (e.g. all the range tokens with "foo"
     * key, a start value > 1 AND an end value <= 10).
     * 
     * @param key
     * @param leftOperator
     * @param left
     * @param joinBy
     * @param rightOperator
     * @param right
     * @return {@code true} if there are any items in the map that match the
     *         filter
     */
    public boolean contains(Text key, Operator leftOperator, Value left,
            JoinBy joinBy, Operator rightOperator, Value right) {
        Shard shard = safeGet(key);
        return shard.contains(leftOperator, left, joinBy, rightOperator, right);
    }

    @Override
    public boolean containsKey(Object obj) {
        return obj instanceof RangeToken ? safeGet(((RangeToken) obj).getKey())
                .containsKey(obj) : false;
    }

    @Override
    public boolean containsValue(Object obj) {
        for (Map<RangeToken, V> map : data.values()) {
            if(map.containsValue(obj)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Entry<RangeToken, V>> entrySet() {
        Set<Entry<RangeToken, V>> entrySet = Sets.newHashSet();
        for (Map<RangeToken, V> shard : data.values()) {
            entrySet.addAll(shard.entrySet());
        }
        return entrySet;
    }

    /**
     * Filter this map to only have range tokens with the specified {@code key}.
     * The returned map is a read-only view to the underlying map.
     * 
     * @param key
     * @return a map that contains the filtered view
     */
    public Map<RangeToken, V> filter(Text key) {
        return safeGet(key);
    }

    /**
     * Filter this map and return one that only has range tokens with the
     * specified {@code key} and a start value that satisfies the
     * {@code startOperator} in relation to the specified {@code start} value
     * joined by an end value that satisfies the {@code endOperator} in relation
     * to the specified {@code end} value (e.g. all the range tokens with "foo"
     * key, a start value > 1 AND an end value <= 10).
     * 
     * @param key
     * @param startOperator
     * @param start
     * @param joinBy
     * @param endOperator
     * @param end
     * @return a collection that contains the filtered view
     */
    public Map<RangeToken, V> filter(Text key, Operator startOperator,
            Value start, JoinBy joinBy, Operator endOperator, Value end) {
        Shard shard = safeGet(key);
        return shard.filter(startOperator, start, joinBy, endOperator, end);
    }

    @Override
    public V get(Object obj) {
        return obj instanceof RangeToken ? safeGet(((RangeToken) obj).getKey())
                .get(obj) : null;
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public Set<RangeToken> keySet() {
        Set<RangeToken> keySet = Sets.newHashSet();
        for (Map<RangeToken, V> map : data.values()) {
            keySet.addAll(map.keySet());
        }
        return keySet;
    }

    @Override
    public V put(RangeToken key, V value) {
        Shard shard = data.get(key.getKey());
        if(shard == null) {
            shard = new Shard();
            data.put(key.getKey(), shard);
        }
        return shard.put(key, value);
    }

    @Override
    public void putAll(Map<? extends RangeToken, ? extends V> map) {
        for (Entry<? extends RangeToken, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public V putIfAbsent(RangeToken key, V value) {
        Shard shard = sureGet(key.getKey());
        return shard.putIfAbsent(key, value);
    }

    @Override
    public V remove(Object key) {
        if(key instanceof RangeToken) {
            Map<RangeToken, V> filtered = safeGet(((RangeToken) key).getKey());
            V value = filtered.remove(key);
            data.remove(((RangeToken) key).getKey(), safeEmptyMap);
            return value;
        }
        else {
            return null;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key instanceof RangeToken) {
            ConcurrentMap<RangeToken, V> filtered = sureGet(((RangeToken) key)
                    .getKey());
            boolean result = filtered.remove(key, value);
            data.remove(((RangeToken) key).getKey(), safeEmptyMap);
            return result;
        }
        else {
            return false;
        }
    }

    @Override
    public V replace(RangeToken key, V value) {
        if(key instanceof RangeToken) {
            ConcurrentMap<RangeToken, V> filtered = sureGet(((RangeToken) key)
                    .getKey());
            return filtered.replace(key, value);
        }
        else {
            return null;
        }
    }

    @Override
    public boolean replace(RangeToken key, V old, V nu) {
        if(key instanceof RangeToken) {
            ConcurrentMap<RangeToken, V> filtered = sureGet(((RangeToken) key)
                    .getKey());
            return filtered.replace(key, old, nu);
        }
        else {
            return false;
        }
    }

    @Override
    public int size() {
        int count = 0;
        for (Map<RangeToken, V> map : data.values()) {
            count += map.size();
        }
        return count;
    }

    @Override
    public Collection<V> values() {
        List<V> values = Lists.newArrayList();
        for (Map<RangeToken, V> map : data.values()) {
            values.addAll(map.values());
        }
        return values;
    }

    /**
     * Safely retrieve the value mapped from {@code key} without having to
     * perform a {@code null} check.
     * 
     * @param key
     * @return
     */
    private Shard safeGet(Text key) {
        Shard shard = data.get(key);
        return shard != null ? shard : safeEmptyMap;
    }

    /**
     * Return the mapping for all the RangeTokens with the specified {@code key}
     * or create it in an atomic way. After this method returns, we can be sure
     * that the mapping is valid.
     * 
     * @param key
     * @return the mapping
     */
    private Shard sureGet(Text key) {
        Shard existing = data.get(key);
        if(existing == null) {
            Shard created = new Shard();
            existing = data.putIfAbsent(key, created);
            existing = Objects.firstNonNull(existing, created);
        }
        return existing;
    }

    /**
     * An enum that determines how to handle the
     * {@link RangeTokenMap#filter(Text, Operator, Value, JoinBy, Operator, Value)}
     * method.
     * 
     * @author jnelson
     */
    public enum JoinBy {
        AND, OR
    }

    /**
     * A shard is internally associated with a {@link Text key} component of a
     * RangeToken. Shards help to perform initial range token filtering. Each
     * shard allows concurrent access that is protected by a {@link StampedLock}
     * and allows further
     * {@link #filter(Operator, Value, JoinBy, Operator, Value) filtering} as
     * well.
     * 
     * @author jnelson
     */
    @ThreadSafe
    private final class Shard implements ConcurrentMap<RangeToken, V> {

        /**
         * The lock that makes this shard ThreadSafe. Access using the
         * {@link #getLock()} method.
         */
        private final StampedLock _lock = new StampedLock();

        /**
         * The entries sorted by their left values.
         */
        private final TreeMap<Range<Value>, Set<Entry<RangeToken, V>>> lefts = Maps
                .newTreeMap(Ranges.<Value> leftValueComparator());

        /**
         * The entries sorted by their right values.
         */
        private final TreeMap<Range<Value>, Set<Entry<RangeToken, V>>> rights = Maps
                .newTreeMap(Ranges.<Value> rightValueComparator());

        @Override
        public void clear() {
            long stamp = getLock().writeLock();
            try {
                lefts.clear();
                rights.clear();
            }
            finally {
                getLock().unlock(stamp);
            }
        }

        /**
         * Check to see if there are values within this shard that match the
         * given filter.
         * 
         * @param leftOperator
         * @param left
         * @param joinBy
         * @param rightOperator
         * @param right
         * @return {@code true} if there mappings that match the filter
         */
        public boolean contains(Operator leftOperator, Value left,
                JoinBy joinBy, Operator rightOperator, Value right) {
            boolean a = checkShardComponent(leftOperator, left, lefts);
            boolean b = checkShardComponent(rightOperator, right, rights);
            return joinBy == JoinBy.AND ? a && b : a || b;
        }

        @Override
        public boolean containsKey(Object key) {
            return keySet().contains(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return values().contains(value);
        }

        @Override
        public Set<Entry<RangeToken, V>> entrySet() {
            long stamp = getLock().tryOptimisticRead();
            Set<Entry<RangeToken, V>> entrySet = entrySet0();
            if(!getLock().validate(stamp)) {
                stamp = getLock().readLock();
                try {
                    return entrySet0();
                }
                finally {
                    getLock().unlock(stamp);
                }
            }
            else {
                return entrySet;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) obj;
                return entrySet().equals(map.entrySet())
                        && entrySet().containsAll(map.entrySet());
            }
            else {
                return false;
            }
        }

        /**
         * Filter within this shard and return a map that satisfies the query.
         * 
         * @param startOperator
         * @param start
         * @param joinBy
         * @param endOperator
         * @param end
         * @return the filtered map
         */
        public Map<RangeToken, V> filter(Operator startOperator, Value start,
                JoinBy joinBy, Operator endOperator, Value end) {
            long stamp = getLock().readLock();
            try {
                Set<Entry<RangeToken, V>> a = filterShardComponent(
                        startOperator, start, lefts);
                Set<Entry<RangeToken, V>> b = filterShardComponent(endOperator,
                        end, rights);
                return joinBy == JoinBy.AND ? TMaps.fromEntrySet(Sets
                        .intersection(a, b)) : TMaps.fromEntrySet(Sets.union(a,
                        b));
            }
            finally {
                getLock().unlock(stamp);
            }

        }

        @Override
        public V get(Object key) {
            if(key instanceof RangeToken) {
                RangeToken token = (RangeToken) key;
                long stamp = getLock(key).tryOptimisticRead();
                V value = get0(token);
                if(!getLock(key).validate(stamp)) {
                    stamp = getLock(key).readLock();
                    try {
                        return get0(token);
                    }
                    finally {
                        getLock(key).unlock(stamp);
                    }
                }
                else {
                    return value;
                }
            }
            else {
                return null;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(lefts, rights);
        }

        @Override
        public boolean isEmpty() {
            long stamp = getLock().tryOptimisticRead();
            boolean empty = lefts.isEmpty() && rights.isEmpty();
            if(!getLock().validate(stamp)) {
                stamp = getLock().readLock();
                try {
                    return lefts.isEmpty() && rights.isEmpty();
                }
                finally {
                    getLock().unlock(stamp);
                }
            }
            else {
                return empty;
            }
        }

        @Override
        public Set<RangeToken> keySet() {
            long stamp = getLock().tryOptimisticRead();
            Set<RangeToken> result = TMaps.extractKeysFromEntrySet(entrySet0());
            if(!getLock().validate(stamp)) {
                stamp = getLock().readLock();
                try {
                    return TMaps.extractKeysFromEntrySet(entrySet0());
                }
                finally {
                    getLock().unlock(stamp);
                }
            }
            else {
                return result;
            }
        }

        @Override
        public V put(RangeToken key, V value) {
            long stamp = getLock().writeLock();
            try {
                return put0(key, value);
            }
            finally {
                getLock().unlock(stamp);
            }
        }

        @Override
        public void putAll(Map<? extends RangeToken, ? extends V> m) {
            long stamp = getLock().writeLock();
            try {
                for (Entry<? extends RangeToken, ? extends V> entry : m
                        .entrySet()) {
                    put0(entry.getKey(), entry.getValue());
                }
            }
            finally {
                getLock().unlock(stamp);
            }
        }

        @Override
        public V putIfAbsent(RangeToken key, V value) {
            long stamp = getLock().writeLock();
            try {
                V existing = get0(key);
                if(existing == null) {
                    put0(key, value);
                }
                return existing;
            }
            finally {
                getLock().unlock(stamp);
            }
        }

        @Override
        public V remove(Object key) {
            if(key instanceof RangeToken) {
                long stamp = getLock(key).writeLock();
                try {
                    return remove0((RangeToken) key);
                }
                finally {
                    getLock().unlock(stamp);
                }
            }
            else {
                return null;
            }
        }

        @Override
        public boolean remove(Object key, Object value) {
            if(key instanceof RangeToken) {
                long stamp = getLock(key).writeLock();
                try {
                    RangeToken token = (RangeToken) key;
                    V existing = get0(token);
                    if(existing.equals(value)) {
                        remove0(token);
                        return true;
                    }
                    else {
                        return false;
                    }
                }
                finally {
                    getLock(key).unlock(stamp);
                }
            }
            else {
                return false;
            }
        }

        @Override
        public V replace(RangeToken key, V value) {
            long stamp = getLock(key).writeLock();
            try {
                RangeToken token = (RangeToken) key;
                V existing = get0(token);
                if(existing != null) {
                    put0(token, value);
                }
                return existing;
            }
            finally {
                getLock(key).unlock(stamp);
            }
        }

        @Override
        public boolean replace(RangeToken key, V oldValue, V newValue) {
            long stamp = getLock(key).writeLock();
            try {
                RangeToken token = (RangeToken) key;
                V existing = get0(token);
                if(existing.equals(oldValue)) {
                    put0(token, newValue);
                    return true;
                }
                else {
                    return false;
                }
            }
            finally {
                getLock(key).unlock(stamp);
            }
        }

        @Override
        public int size() {
            long stamp = getLock().tryOptimisticRead();
            int result = Math.max(lefts.size(), rights.size());
            if(!getLock().validate(stamp)) {
                stamp = getLock().readLock();
                try {
                    return Math.max(lefts.size(), rights.size());
                }
                finally {
                    getLock().unlock(stamp);
                }
            }
            else {
                return result;
            }
        }

        @Override
        public String toString() {
            return Maps.newHashMap(this).toString();
        }

        @Override
        public Collection<V> values() {
            long stamp = getLock().tryOptimisticRead();
            Collection<V> result = TMaps.extractValuesFromEntrySet(entrySet0());
            if(!getLock().validate(stamp)) {
                stamp = getLock().readLock();
                try {
                    return TMaps.extractValuesFromEntrySet(entrySet0());
                }
                finally {
                    getLock().unlock(stamp);
                }
            }
            else {
                return result;
            }
        }

        /**
         * Return the entry set for this shard without grabbing any locks.
         * 
         * @return the entry set
         */
        private Set<Entry<RangeToken, V>> entrySet0() {
            return TSets.intersection(
                    Sets.newHashSet(MultimapViews.values(lefts)),
                    Sets.newHashSet(MultimapViews.values(rights)));
        }

        /**
         * Perform the routine to get the value associated with the
         * {@code token}, if it exists, without grabbing any locks.
         * 
         * @param token
         * @return the value associated with the {@code token}
         */
        private V get0(RangeToken key) {
            Iterable<Range<Value>> ranges = RangeTokens.convertToRange(key);
            Set<Entry<RangeToken, V>> entries = null;
            for (Range<Value> range : ranges) {
                Set<Entry<RangeToken, V>> latest = Sets.intersection(
                        MultimapViews.get(lefts, range),
                        MultimapViews.get(rights, range));
                entries = entries == null ? latest : Sets.intersection(entries,
                        latest);
            }
            entries = Sets.newHashSet(entries);
            Iterator<Entry<RangeToken, V>> it = entries.iterator();
            while (it.hasNext()) {
                // Handle case like foo = bar and far AS bar are both stored in
                // the map, pointing to different values
                Entry<RangeToken, V> entry = it.next();
                RangeToken token = entry.getKey();
                if(!((key.getOperator() == null && token.getOperator() == null) || (key
                        .getOperator() != null && token.getOperator() != null && key
                        .getOperator().equals(token.getOperator())))) {
                    it.remove();
                }
            }
            Entry<RangeToken, V> entry;
            return (entry = Iterables.getOnlyElement(entries, null)) != null ? entry
                    .getValue() : null;

        }

        /**
         * Alias to the {@link #getLock(Object)} method.
         * 
         * @return the lock
         */
        private StampedLock getLock() {
            return getLock(null);
        }

        /**
         * Get the appropriate lock that corresponds to the {@code key}.
         * 
         * @param key
         * @return the lock
         */
        private StampedLock getLock(@Nullable Object key) {
            return key == null || key instanceof RangeToken ? _lock : Locks
                    .noOpStampedLock();
        }

        /**
         * Map {@code key} to {@code value} within this shard without grabbing
         * any locks.
         * 
         * @param key
         * @param value
         * @return the previous value that was mapped from {@code key}, if it
         *         exists
         */
        @Nullable
        private V put0(RangeToken key, V value) {
            Iterable<Range<Value>> ranges = RangeTokens.convertToRange(key);
            Entry<RangeToken, V> entry = new AbstractMap.SimpleImmutableEntry<RangeToken, V>(
                    key, value);
            V previous = get0(key);
            if(previous != null) {
                remove0(key);
            }
            for (Range<Value> range : ranges) {
                MultimapViews.put(lefts, range, entry);
                MultimapViews.put(rights, range, entry);
            }
            return previous;
        }

        /**
         * Remove the mapping from {@code key} to whatever value is stored
         * within this shard without grabbing any locks.
         * 
         * @param key
         * @return the previous valued that was mapped from {@code key}, if it
         *         exists
         */
        @Nullable
        private V remove0(RangeToken key) {
            V value = get0(key);
            if(value != null) {
                Iterable<Range<Value>> ranges = RangeTokens.convertToRange(key);
                Entry<RangeToken, V> entry = new AbstractMap.SimpleImmutableEntry<RangeToken, V>(
                        key, value);
                for (Range<Value> range : ranges) {
                    MultimapViews.remove(lefts, range, entry);
                    MultimapViews.remove(rights, range, entry);
                }
            }
            return value;
        }

    }

}
