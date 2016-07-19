package com.cinchapi.concourse.plugin.model;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CharacterizingMap<K, V> implements Map<K, Set<V>> {
    
    private Map<K, Set<V>> internalMap;
    
    public CharacterizingMap() {
        internalMap = new ConcurrentHashMap<K, Set<V>>();
    }

    @Override
    public int size() {
        return internalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return internalMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public Set<V> get(Object key) {
        return internalMap.get(key);
    }

    @Override
    public Set<V> put(K key, Set<V> value) {
        // TODO Calculate/characterize
        return internalMap.put(key, value);
    }

    @Override
    public Set<V> remove(Object key) {
        // TODO Calculate/characterize
        return internalMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends Set<V>> m) {
        for(K key : m.keySet()) {
            put(key, m.get(key));
        }
    }

    @Override
    public void clear() {
        for(K key : internalMap.keySet()) {
            remove(key);
        }
    }

    @Override
    public Set<K> keySet() {
        return internalMap.keySet();
    }

    @Override
    public Collection<Set<V>> values() {
        return internalMap.values();
    }

    @Override
    public Set<Entry<K, Set<V>>> entrySet() {
        return internalMap.entrySet();
    }
    

}
