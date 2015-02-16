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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.vendor.jsr166e.ConcurrentHashMapV8;
import org.cinchapi.vendor.jsr166e.StampedLock;

import com.google.common.base.Objects;
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
     * A mapping from range token keys to the appropriate shard, which mimics a
     * mapping from RangeToken to V in order to make filtering operations more
     * efficient.
     * 
     */
    private final ConcurrentMap<Text, Shard> shards = new ConcurrentHashMapV8<Text, Shard>();

    /**
     * An empty map that is returned in the {@link #safeGet()} method so that
     * callers can read through to it without having to perform a {@code null}
     * check.
     */
    private Shard safeEmptyMap = new Shard();

    @Override
    public void clear() {
        shards.clear();
    }

<<<<<<< HEAD
    /**
     * TODO
     * 
     * @param key
     * @param operator
     * @param value
     * @return
     */
    public boolean contains(Text key, Operator operator, Value value) {
        Shard shard = safeGet(key);
        return shard.contains(operator, value);
    }

    /**
     * TODO
     * 
     * @param key
     * @param value1
     * @param value2
     * @return
     */
    public boolean contains(Text key, Value value1, Value value2) {
        Shard shard = safeGet(key);
        return shard.contains(Operator.BETWEEN, value1, value2);
=======
    public boolean contains(RangeToken token, Condition<RangeToken, V> condition) {
        return safeGet(token.getKey()).contains(token, condition);
    }

    public void remove(RangeToken token, Condition<RangeToken, V> condition,
            AfterRemoval<RangeToken, V> afterwards) {
        safeGet(token.getKey()).remove(token, condition, afterwards);
    }

    public boolean contains(Text key, Condition<RangeToken, V> condition) {
        return safeGet(key).contains(condition);
>>>>>>> c988f85... lots of changes to RangeTokenMap and RangeToken and Range, etc
    }

    @Override
    public boolean containsKey(Object obj) {
        return obj instanceof RangeToken ? safeGet(((RangeToken) obj).getKey())
                .containsKey(obj) : false;
    }

    @Override
    public boolean containsValue(Object obj) {
        for (Shard shard : shards.values()) {
            if(shard.containsValue(obj)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Entry<RangeToken, V>> entrySet() {
        Set<Entry<RangeToken, V>> entrySet = Sets.newHashSet();
        for (Map<RangeToken, V> shard : shards.values()) {
            entrySet.addAll(shard.entrySet());
        }
        return entrySet;
    }

<<<<<<< HEAD
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
     * Filter this map and return... TODO update
     * 
     * @param key
     * @param operator
     * @param value
     * @return the filtered view
     */
    public Map<RangeToken, V> filter(Text key, Operator operator, Value value) {
        Shard shard = safeGet(key);
        return shard.filter(operator, value);
    }

    /**
     * Filter this map to return all the entries where the range token has the
     * specified {@code key} component} and the value(s) in the token is/are
     * between {@code value1} and {@code value2}.
     * 
     * @param key
     * @param value1
     * @param value2
     * @return the filtered view
     */
    public Map<RangeToken, V> filter(Text key, Value value1, Value value2) {
        Shard shard = safeGet(key);
        return shard.filter(Operator.BETWEEN, value1, value2);
    }

=======
>>>>>>> c988f85... lots of changes to RangeTokenMap and RangeToken and Range, etc
    @Override
    public V get(Object obj) {
        return obj instanceof RangeToken ? safeGet(((RangeToken) obj).getKey())
                .get(obj) : null;
    }

    @Override
    public boolean isEmpty() {
        return shards.isEmpty();
    }

    @Override
    public Set<RangeToken> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(RangeToken key, V value) {
        Shard shard = shards.get(key.getKey());
        if(shard == null) {
            shard = new Shard();
            shards.put(key.getKey(), shard);
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
            shards.remove(((RangeToken) key).getKey(), safeEmptyMap);
            return value;
        }
        else {
            return null;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        if(key instanceof RangeToken) {
            Shard shard = sureGet(((RangeToken) key).getKey());
            boolean result = shard.remove(key, value);
            shards.remove(((RangeToken) key).getKey(), safeEmptyMap);
            return result;
        }
        else {
            return false;
        }
    }

    @Override
    public V replace(RangeToken key, V value) {
        if(key instanceof RangeToken) {
            Shard shard = sureGet(((RangeToken) key).getKey());
            return shard.replace(key, value);
        }
        else {
            return null;
        }
    }

    @Override
    public boolean replace(RangeToken key, V old, V nu) {
        if(key instanceof RangeToken) {
            Shard shard = sureGet(((RangeToken) key).getKey());
            return shard.replace(key, old, nu);
        }
        else {
            return false;
        }
    }

    @Override
    public int size() {
        int count = 0;
        for (Map<RangeToken, V> map : shards.values()) {
            count += map.size();
        }
        return count;
    }

    @Override
    public Collection<V> values() {
        List<V> values = Lists.newArrayList();
        for (Map<RangeToken, V> map : shards.values()) {
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
        Shard shard = shards.get(key);
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
        Shard existing = shards.get(key);
        if(existing == null) {
            Shard created = new Shard();
            existing = shards.putIfAbsent(key, created);
            existing = Objects.firstNonNull(existing, created);
        }
        return existing;
    }

    public static interface Condition<K, V> {

        public boolean satisfiedBy(K key, V value);

    }

    public static interface AfterRemoval<K, V> {

        public void clean(K key, V value);
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

        private final Map<RangeToken, V> hashed = Maps.newHashMap();
        private final StampedLock lock = new StampedLock();

        /**
         * Return {@code true} if there is any entry in this shard that matches
         * the {@code condition}.
         * 
         * @param condition
         * @return {@code true} if the condition is met
         */
        public boolean contains(Condition<RangeToken, V> condition) {
            Iterator<Entry<RangeToken, V>> it = hashed.entrySet().iterator();
            while (it.hasNext()) {
                Entry<RangeToken, V> entry = it.next();
                if(condition.satisfiedBy(entry.getKey(), entry.getValue())) {
                    return true;
                }
            }
            return false;
        }

        public void remove(RangeToken token,
                Condition<RangeToken, V> condition,
                AfterRemoval<RangeToken, V> afterwards) {
            Iterator<Entry<RangeToken, V>> it = hashed.entrySet().iterator();
            while (it.hasNext()) {
                Entry<RangeToken, V> entry = it.next();
                if(condition.satisfiedBy(entry.getKey(), entry.getValue())
                        && entry.getKey().intersects(token)) {
                    it.remove();
                    afterwards.clean(entry.getKey(), entry.getValue());
                }
            }
        }

        public boolean contains(RangeToken token,
                Condition<RangeToken, V> condition) {
            Iterator<Entry<RangeToken, V>> it = hashed.entrySet().iterator();
            while (it.hasNext()) {
                Entry<RangeToken, V> entry = it.next();
                if(condition.satisfiedBy(entry.getKey(), entry.getValue())
                        && entry.getKey().intersects(token)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int size() {
            long stamp = lock.readLock();
            try {
                return hashed.size();
            }
            finally {
                lock.unlock(stamp);
            }
        }

<<<<<<< HEAD
        /**
         * TODO
         * 
         * @param operator
         * @param value
         * @return
         */
        public boolean contains(Operator operator, Value value) {
            long stamp = getLock().readLock();
=======
        @Override
        public boolean isEmpty() {
            long stamp = lock.readLock();
>>>>>>> c988f85... lots of changes to RangeTokenMap and RangeToken and Range, etc
            try {
                return hashed.isEmpty();
            }
            finally {
                lock.unlock(stamp);
            }
        }

<<<<<<< HEAD
        /**
         * TODO
         * 
         * @param operator
         * @param value1
         * @param value2
         * @return
         */
        public boolean contains(Operator operator, Value value1, Value value2) {
            long stamp = getLock().readLock();
=======
        @Override
        public boolean containsKey(Object key) {
            long stamp = lock.readLock();
>>>>>>> c988f85... lots of changes to RangeTokenMap and RangeToken and Range, etc
            try {
                return hashed.containsKey(key);
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public boolean containsValue(Object value) {
            long stamp = lock.readLock();
            try {
                return hashed.containsValue(value);
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public V get(Object key) {
            long stamp = lock.readLock();
            try {
                return hashed.get(key);
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public V put(RangeToken key, V value) {
            long stamp = lock.writeLock();
            try {
                return hashed.put(key, value);
            }
            finally {
                lock.unlock(stamp);
            }
        }

        /**
         * Do the work to filter this shard to return the entry set where all
         * the range tokens have values between {@code value1} and
         * {@code value2}.
         * 
         * @param operator
         * @param value1
         * @param value2
         * @return the filtered entry set
         */
        private Set<Entry<RangeToken, V>> filter0(Operator operator,
                Value value1, Value value2) {
            Preconditions.checkArgument(operator == Operator.BETWEEN);
            Range<Value> point1 = Range.point(value1);
            Range<Value> point2 = Range.point(value2);
            Set<Entry<RangeToken, V>> a = Sets.newHashSet(Iterables
                    .concat(lefts.headMap(point2, false).values()));
            Set<Entry<RangeToken, V>> b = Sets.newHashSet(Iterables
                    .concat(rights.tailMap(point1, true).values()));
            return Sets.intersection(a, b);
        }

        /**
         * TODO
         * 
         * @param operator
         * @param value1
         * @param value2
         * @return
         */
        private boolean contains0(Operator operator, Value value1, Value value2) {
            Preconditions.checkArgument(operator == Operator.BETWEEN);
            return !lefts.headMap(Range.point(value2), false).values()
                    .isEmpty()
                    && !rights.tailMap(Range.point(value1), true).values()
                            .isEmpty();
        }

        /**
         * Do the work to filter this shard and return the entry set where all
         * the range tokens have values that satisfy {@code operator} in
         * relation to the specified {@code value}.
         * 
         * @param operator
         * @param value
         * @return the filtered entry set
         */
        private Set<Entry<RangeToken, V>> filter0(Operator operator, Value value) {
            Range<Value> point = Range.point(value);
            switch (operator) {
            case EQUALS:
                return Sets.intersection(lefts.get(point), rights.get(point));
            case NOT_EQUALS:
                Set<Entry<RangeToken, V>> ne = Sets.newHashSet();
                for (Set<Entry<RangeToken, V>> coll : lefts.tailMap(point,
                        false).values()) {
                    for (Entry<RangeToken, V> entry : coll) {
                        Iterable<Range<Value>> ranges = RangeTokens
                                .convertToRange(entry.getKey());
                        boolean add = true;
                        for (Range<Value> range : ranges) {
                            if(range.intersects(point)) {
                                add = false;
                                break;
                            }
                        }
                        if(add) {
                            ne.add(entry);
                        }
                    }
                }
                for (Set<Entry<RangeToken, V>> coll : rights.headMap(point,
                        false).values()) {
                    for (Entry<RangeToken, V> entry : coll) {
                        Iterable<Range<Value>> ranges = RangeTokens
                                .convertToRange(entry.getKey());
                        boolean add = true;
                        for (Range<Value> range : ranges) {
                            if(range.intersects(point)) {
                                add = false;
                                break;
                            }
                        }
                        if(add) {
                            ne.add(entry);
                        }
                    }
                }
                return ne;
            case GREATER_THAN:
                return Sets.newHashSet(Iterables.concat(rights.tailMap(point,
                        false).values()));
            case GREATER_THAN_OR_EQUALS:
                return Sets.newHashSet(Iterables.concat(rights.tailMap(point,
                        true).values()));
            case LESS_THAN:
                return Sets.newHashSet(Iterables.concat(lefts.headMap(point,
                        false).values()));
            case LESS_THAN_OR_EQUALS:
                return Sets.newHashSet(Iterables.concat(lefts.headMap(point,
                        true).values()));
            default:
                throw new UnsupportedOperationException();
            }
        }

        /**
         * TODO
         * 
         * @param operator
         * @param value
         * @return
         */
        private boolean contains0(Operator operator, Value value) {
            Range<Value> point = Range.point(value);
            switch (operator) {
            case EQUALS:
                Set<Entry<RangeToken, V>> fromLeft;
                Set<Entry<RangeToken, V>> fromRight;
                return (fromLeft = lefts.get(point)) != null
                        && !fromLeft.isEmpty()
                        && (fromRight = rights.get(point)) != null
                        && !fromRight.isEmpty();
            case NOT_EQUALS:
                // TODO better way to do this...
                Set<Entry<RangeToken, V>> ne = Sets.newHashSet();
                for (Set<Entry<RangeToken, V>> coll : lefts.tailMap(point,
                        false).values()) {
                    for (Entry<RangeToken, V> entry : coll) {
                        Iterable<Range<Value>> ranges = RangeTokens
                                .convertToRange(entry.getKey());
                        boolean add = true;
                        for (Range<Value> range : ranges) {
                            if(range.intersects(point)) {
                                add = false;
                                break;
                            }
                        }
                        if(add) {
                            ne.add(entry);
                        }
                    }
                }
                for (Set<Entry<RangeToken, V>> coll : rights.headMap(point,
                        false).values()) {
                    for (Entry<RangeToken, V> entry : coll) {
                        Iterable<Range<Value>> ranges = RangeTokens
                                .convertToRange(entry.getKey());
                        boolean add = true;
                        for (Range<Value> range : ranges) {
                            if(range.intersects(point)) {
                                add = false;
                                break;
                            }
                        }
                        if(add) {
                            ne.add(entry);
                        }
                    }
                }
                return !ne.isEmpty();
            case GREATER_THAN:
                return !rights.tailMap(point, false).values().isEmpty();
            case GREATER_THAN_OR_EQUALS:
                return !rights.tailMap(point, true).values().isEmpty();
            case LESS_THAN:
                return !lefts.headMap(point, false).values().isEmpty();
            case LESS_THAN_OR_EQUALS:
                return !lefts.headMap(point, true).values().isEmpty();
            default:
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public V remove(Object key) {
            if(key instanceof RangeToken) {
                long stamp = lock.writeLock();
                try {
                    return hashed.remove((RangeToken) key);
                }
                finally {
                    lock.unlock(stamp);
                }
            }
            else {
                return null;
            }
        }

        @Override
        public void putAll(Map<? extends RangeToken, ? extends V> m) {
            long stamp = lock.writeLock();
            try {
                for (Entry<? extends RangeToken, ? extends V> entry : m
                        .entrySet()) {
                    hashed.put(entry.getKey(), entry.getValue());
                }
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public void clear() {
            long stamp = lock.writeLock();
            try {
                hashed.clear();
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public Set<RangeToken> keySet() {
            long stamp = lock.readLock();
            try {
                return hashed.keySet();
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public Collection<V> values() {
            long stamp = lock.readLock();
            try {
                return hashed.values();
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public Set<Entry<RangeToken, V>> entrySet() {
            long stamp = lock.readLock();
            try {
                return hashed.entrySet();
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public V putIfAbsent(RangeToken key, V value) {
            long stamp = lock.writeLock();
            try {
                V stored = hashed.get(key);
                if(stored == null) {
                    hashed.put(key, value);
                }
                return stored;
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
        public boolean remove(Object key, Object value) {
            if(key instanceof RangeToken) {
                long stamp = lock.writeLock();
                try {
                    V stored = hashed.get(key);
                    if(stored != null && stored.equals(value)) {
                        hashed.remove(key);
                    }
                    return true;
                }
                finally {
                    lock.unlock(stamp);
                }
            }
            else {
                return false;
            }
        }

        @Override
        public boolean replace(RangeToken key, V oldValue, V newValue) {
            long stamp = lock.writeLock();
            try {
                V stored = hashed.get(key);
                if(stored != null && stored.equals(oldValue)) {
                    hashed.put(key, newValue);
                    return true;
                }
                else {
                    return false;
                }
            }
            finally {
                lock.unlock(stamp);
            }
        }

        @Override
<<<<<<< HEAD
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
            while (cachedEntrySet == null) {
                cachedEntrySet = TSets.intersection(
                        Sets.newHashSet(MultimapViews.values(lefts)),
                        Sets.newHashSet(MultimapViews.values(rights)));
            }
            return cachedEntrySet;
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
=======
        public V replace(RangeToken key, V value) {
            long stamp = lock.writeLock();
            try {
                V stored = hashed.get(key);
                if(stored != null) {
                    hashed.put(key, value);
>>>>>>> c988f85... lots of changes to RangeTokenMap and RangeToken and Range, etc
                }
                return stored;
            }
            finally {
                lock.unlock(stamp);
            }
        }
    }

}
