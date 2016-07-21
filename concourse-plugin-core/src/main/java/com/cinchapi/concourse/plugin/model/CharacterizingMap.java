package com.cinchapi.concourse.plugin.model;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of a {@code Map} that characterizes entries on the fly as they are added or
 * removed. This is used to characterize user data as it is being entered, so that the visualization
 * engine can query this map to immediately view data characterization in constant time.
 * 
 * Apart from {@code #put(Object, Set)}, {@code #remove(Object)}, {@code #putAll(Map)}, and {@code #clear()},
 * all methods are delegated to an internal map. The four aforementioned methods are overridden in terms of
 * functionality to characterize the entries in the map before performing the original intended function.
 * 
 * {@link CharacterizingMap} is parametrized by type-parameters K and V, but the underlying internal map is
 * in the form {@code Map<K, Set<V>>}. This is to comply with the format of data, which is either a Map from
 * Strings (keys) to Objects (values), or Objects (values) to Longs (records).
 * 
 * @author Aditya Srinivasan
 *
 * @param <K>
 * @param <V>
 */
public class CharacterizingMap<K, V> implements Map<K, Set<V>> {
    
    /**
     * The internal map towards which delegations are assigned.
     */
    private Map<K, Set<V>> delegate;
    
    /**
     * Constructs a new instance of a {@code CharacterizingMap}.
     */
    public CharacterizingMap() {
        delegate = new ConcurrentHashMap<K, Set<V>>();
    }

    /**
     * Returns the number of key-value mappings in this map.
     * If the map contains more than {@code Integer.MAX_VALUE}
     * elements, returns {@code Integer.MAX_VALUE}.
     */
    @Override
    public int size() {
        return delegate.size();
    }
    
    /**
     * Returns true if this map contains no key-value mappings.
     */
    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /**
     * Returns {@code true} if this map contains a mapping for the specified
     * key. More formally, returns {@code true} if and only if this map contains
     * a mapping for a key {@code k} such that {@code (key==null ? k==null : key.equals(k))}.
     * (There can be at most one such mapping.)
     */
    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the specified value.
     * More formally, returns {@code true} if and only if this map contains at least
     * one mapping to a value {@code v} such that {@code (value==null ? v==null : value.equals(v))}}.
     * This operation will probably require time linear in the map size for most
     * implementations of the {@code Map} interface.
     * 
     */
    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    /**
     * Returns the value to which the specified key is mapped, or null
     * if this map contains no mapping for the key.
     */
    @Override
    public Set<V> get(Object key) {
        return delegate.get(key);
    }

    /**
     * Characterizes the key and value along with existing entries, and
     * associates the specified value with the specified key in this map
     * (optional operation). If the map previously contained a mapping
     * for the key, the old value is replaced by the specified value.
     */
    @Override
    public Set<V> put(K key, Set<V> value) {
        // TODO Calculate/characterize
        return delegate.put(key, value);
    }

    /**
     * Removes the mapping for a key from this map if it is present
     * (optional operation). Also accounts for removal of key and
     * value in recalculation of data characterization before
     * removal.
     * 
     * Returns the value to which this map previously associated the
     * key, or {@code null} if the map contained no mapping for the key.
     * 
     * If this map permits {@code null} values, then a return value of {@code null}
     * does not necessarily indicate that the map contained no mapping
     * for the key; it's also possible that the map explicitly mapped
     * the key to {@code null}.
     * 
     * The map will not contain a mapping for the specified key once
     * the call returns.
     */
    @Override
    public Set<V> remove(Object key) {
        // TODO Calculate/characterize
        return delegate.remove(key);
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * (optional operation). The effect of this call is equivalent to that
     * of calling {@link #put(k, v)} on this map once for each mapping from key {@code k} to
     * value {@code v} in the specified map. The behavior of this operation is
     * undefined if the specified map is modified while the operation is
     * in progress.
     * 
     */
    @Override
    public void putAll(Map<? extends K, ? extends Set<V>> m) {
        for(K key : m.keySet()) {
            put(key, m.get(key));
        }
    }

    /**
     * Removes all of the mappings from this map (optional operation).
     * The map will be empty after this call returns and all characterization
     * will be reset.
     */
    @Override
    public void clear() {
        for(K key : delegate.keySet()) {
            remove(key);
        }
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map. The set is backed
     * by the map, so changes to the map are reflected in the set, and vice-versa.
     * If the map is modified while an iteration over the set is in progress
     * (except through the iterator's own remove operation), the results of the
     * iteration are undefined.
     * 
     */
    @Override
    public Set<K> keySet() {
        return delegate.keySet();
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map. The
     * collection is backed by the map, so changes to the map are reflected in the 
     * collection, and vice-versa. If the map is modified while an iteration over the
     * collection is in progress (except through the iterator's own remove operation),
     * the results of the iteration are undefined. 
     */
    @Override
    public Collection<Set<V>> values() {
        return delegate.values();
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map. The set is
     * backed by the map, so changes to the map are reflected in the set, and vice-versa.
     * If the map is modified while an iteration over the set is in progress (except
     * through the iterator's own remove operation, or through the setValue operation
     * on a map entry returned by the iterator) the results of the iteration are undefined. 
     */
    @Override
    public Set<Entry<K, Set<V>>> entrySet() {
        return delegate.entrySet();
    }
    

}
