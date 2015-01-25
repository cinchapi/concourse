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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.common.util.IncrementalSortMap;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.vendor.jsr166e.ConcurrentHashMapV8;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
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
     * A mapping from range token keys to the appropriate range token -> V
     * mappings.
     */
    private final ConcurrentMap<Text, ConcurrentMap<RangeToken, V>> data = new ConcurrentHashMapV8<Text, ConcurrentMap<RangeToken, V>>();

    /**
     * An empty map that is returned in the {@link #safeGet()} method so that
     * callers can read through to it without having to perform a {@code null}
     * check.
     */
    private ConcurrentMap<RangeToken, V> safeEmptyMap = IncrementalSortMap
            .create(RangeTokens.APPROX_VALUE_COMPARATOR);

    @Override
    public void clear() {
        data.clear();
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
        for (Map<RangeToken, V> map : data.values()) {
            entrySet.addAll(map.entrySet());
        }
        return entrySet;
    }

    /**
     * Filter this map to only have range tokens with the specified {@code key}.
     * The returned map is a read-only view to the underlying map.
     * 
     * @param key
     * @return a filtered view
     */
    public Map<RangeToken, V> filter(Text key) {
        return Collections.unmodifiableMap(safeGet(key));
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
        ConcurrentMap<RangeToken, V> filtered = data.get(key.getKey());
        if(filtered == null) {
            filtered = IncrementalSortMap
                    .create(RangeTokens.APPROX_VALUE_COMPARATOR);
            data.put(key.getKey(), filtered);
        }
        return filtered.put(key, value);
    }

    @Override
    public void putAll(Map<? extends RangeToken, ? extends V> map) {
        for (Entry<? extends RangeToken, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public V putIfAbsent(RangeToken key, V value) {
        ConcurrentMap<RangeToken, V> filtered = sureGet(key.getKey());
        return filtered.putIfAbsent(key, value);
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
    private ConcurrentMap<RangeToken, V> safeGet(Text key) {
        ConcurrentMap<RangeToken, V> filtered = data.get(key);
        return filtered != null ? filtered : safeEmptyMap;
    }

    /**
     * Return the mapping for all the RangeTokens with the specified {@code key}
     * or create it in an atomic way. After this method returns, we can be sure
     * that the mapping is valid.
     * 
     * @param key
     * @return the mapping
     */
    private ConcurrentMap<RangeToken, V> sureGet(Text key) {
        ConcurrentMap<RangeToken, V> existing = data.get(key);
        if(existing == null) {
            ConcurrentMap<RangeToken, V> created = IncrementalSortMap
                    .create(RangeTokens.APPROX_VALUE_COMPARATOR);
            existing = data.putIfAbsent(key, created);
            existing = Objects.firstNonNull(existing, created);
        }
        return existing;
    }

}
