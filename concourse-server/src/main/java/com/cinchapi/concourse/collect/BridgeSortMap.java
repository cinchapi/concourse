/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.collect;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;

/**
 * A {@link SortedMap} wrapper that facilitates efficient modifications to an
 * apparently sorted map (even if it doesn't implement the {@link SortedMap}
 * interface). The {@link BridgeSortMap} maintains sorted order according to a
 * specified {@link Comparator}.
 * <p>
 * The {@link BridgeSortMap} assumes that the provided map already contains
 * entries in sorted order according to the specified {@link Comparator}. This
 * assumption is not verified, so it is the caller's responsibility to ensure
 * the provided map's entries are properly ordered.
 * <p>
 * New entries are sorted on the fly and, during iteration, the map efficiently
 * combines the presumed sorted order of the original entries with the
 * guaranteed sorted order of any new entries, presenting a unified sorted view
 * without resorting the entire collection.
 * <p>
 * This approach is particularly useful in scenarios where:
 * <ul>
 * <li>You need to maintain a sorted view while adding new entries</li>
 * <li>You want to avoid the performance cost of resorting a large
 * collection</li>
 * <li>You need to efficiently modify a sorted collection</li>
 * </ul>
 * <p>
 * Note that some operations like {@link #subMap(Object, Object)},
 * {@link #headMap(Object)}, {@link #tailMap(Object)}, {@link #firstKey()}, and
 * {@link #lastKey()} are not supported in the current implementation.
 *
 * @author Jeff Nelson
 */
public class BridgeSortMap<K, V> implements SortedMap<K, V> {

    /**
     * The base map that is assumed to contain entries in sorted order according
     * to
     * the specified {@link Comparator}.
     */
    private final Map<K, V> provided;

    /**
     * A {@link SortedMap} that stores new entries added after construction.
     * This map guarantees that its entries are sorted according to the
     * specified
     * {@link Comparator}.
     */
    private final SortedMap<K, V> incremental;

    /**
     * The {@link Comparator} that defines the sort order for keys in this map.
     */
    private final Comparator<? super K> comparator;

    /**
     * Construct a new instance.
     * 
     * @param provided
     * @param comparator
     */
    public BridgeSortMap(@Nonnull Map<K, V> provided,
            Comparator<? super K> comparator) {
        this.provided = provided;
        this.comparator = comparator;
        this.incremental = new TreeMap<>(comparator);
    }

    @Override
    public void clear() {
        provided.clear();
        incremental.clear();

    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public boolean containsKey(Object key) {
        return provided.containsKey(key) || incremental.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return provided.containsValue(value)
                || incremental.containsValue(value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Map) {
            Map<K, V> map = (Map<K, V>) obj;
            return map.entrySet().equals(entrySet());
        }
        else {
            return false;
        }
    }

    @Override
    public K firstKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        V value = provided.get(key);
        if(value == null) {
            value = incremental.get(key);
        }
        return value;
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return provided.isEmpty() && incremental.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return new KeySet();
    }

    @Override
    public K lastKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V put(K key, V value) {
        if(provided.containsKey(key)) {
            return provided.put(key, value);
        }
        else {
            return incremental.put(key, value);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.forEach((key, value) -> put(key, value));
    }

    @Override
    public V remove(Object key) {
        if(provided.containsKey(key)) {
            return provided.remove(key);
        }
        else {
            return incremental.remove(key);
        }
    }

    @Override
    public int size() {
        return provided.size() + incremental.size();
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        Iterator<Entry<K, V>> i = entrySet().iterator();
        if(!i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (;;) {
            Entry<K, V> e = i.next();
            K key = e.getKey();
            V value = e.getValue();
            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if(!i.hasNext())
                return sb.append('}').toString();
            sb.append(',').append(' ');
        }
    }

    @Override
    public Collection<V> values() {
        return new Values();
    }

    /**
     * A set view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa. The set supports element
     * removal, which removes the corresponding mapping from the map.
     */
    private class EntrySet extends AbstractSet<Entry<K, V>> {

        @Override
        public boolean add(Entry<K, V> e) {
            K key = e.getKey();
            V value = BridgeSortMap.this.get(e.getKey());
            if(!value.equals(e.getValue())) {
                BridgeSortMap.this.put(key, e.getValue());
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public boolean addAll(Collection<? extends Entry<K, V>> c) {
            boolean ret = false;
            for (Entry<K, V> e : c) {
                if(add(e)) {
                    ret = true;
                }
            }
            return ret;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new Iterator<Entry<K, V>>() {
                Iterator<Entry<K, V>> pit;
                Iterator<Entry<K, V>> iit;
                Entry<K, V> p;
                Entry<K, V> i;
                {
                    pit = provided.entrySet().iterator();
                    iit = incremental.entrySet().iterator();
                    p = pit.hasNext() ? pit.next() : null;
                    i = iit.hasNext() ? iit.next() : null;
                }

                @Override
                public boolean hasNext() {
                    return p != null || i != null;
                }

                @Override
                public Entry<K, V> next() {
                    Entry<K, V> next;
                    if(p == null && i == null) {
                        throw new NoSuchElementException();
                    }
                    else if(i == null) {
                        next = p;
                        p = pit.hasNext() ? pit.next() : null;
                    }
                    else if(p == null) {
                        next = i;
                        i = iit.hasNext() ? iit.next() : null;
                    }
                    else {
                        int c = comparator.compare(p.getKey(), i.getKey());
                        if(c < 0) {
                            next = p;
                            p = pit.hasNext() ? pit.next() : null;
                        }
                        else if(c > 0) {
                            next = i;
                            i = iit.hasNext() ? iit.next() : null;
                        }
                        else {
                            throw new IllegalStateException(
                                    "A value exists in both the provided and incremental shards");
                        }
                    }
                    return next;
                }

            };
        }

        @Override
        public int size() {
            return BridgeSortMap.this.size();
        }

    }

    /**
     * A set view of the keys contained in this map. The set is backed by the
     * map, so changes to the map are reflected in the set, and vice-versa.
     */
    private class KeySet extends AbstractSet<K> {

        @Override
        public void clear() {
            BridgeSortMap.this.clear();
        }

        @Override
        public boolean contains(Object k) {
            return BridgeSortMap.this.containsKey(k);
        }

        @Override
        public Iterator<K> iterator() {
            return new Iterator<K>() {
                private Iterator<Entry<K, V>> i = entrySet().iterator();

                public boolean hasNext() {
                    return i.hasNext();
                }

                public K next() {
                    return i.next().getKey();
                }

                public void remove() {
                    i.remove();
                }
            };
        }

        @Override
        public int size() {
            return BridgeSortMap.this.size();
        }

    }

    /**
     * A collection view of the values contained in this map. The collection is
     * backed by the map, so changes to the map are reflected in the collection,
     * and vice-versa.
     */
    private class Values extends AbstractCollection<V> {

        @Override
        public void clear() {
            BridgeSortMap.this.clear();
        }

        @Override
        public boolean contains(Object v) {
            return BridgeSortMap.this.containsValue(v);
        }

        @Override
        public boolean isEmpty() {
            return BridgeSortMap.this.isEmpty();
        }

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {
                private Iterator<Entry<K, V>> i = entrySet().iterator();

                public boolean hasNext() {
                    return i.hasNext();
                }

                public V next() {
                    return i.next().getValue();
                }

                public void remove() {
                    i.remove();
                }
            };
        }

        @Override
        public int size() {
            return BridgeSortMap.this.size();
        }

    }

}
