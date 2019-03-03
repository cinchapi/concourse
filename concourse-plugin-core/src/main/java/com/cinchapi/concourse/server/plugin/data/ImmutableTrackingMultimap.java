/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.plugin.data;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A {@link TrackingMultimap} that provides read-only access.
 *
 * @author Jeff Nelson
 */
public class ImmutableTrackingMultimap<K, V> extends TrackingMultimap<K, V> {

    // NOTE: Write methods have been annotated as @Deprecated to leverage IDE
    // hints that the methods shouldn't be used (i.e. eclipse will show the
    // method names with a strikethrough)

    /**
     * Create a {@link ImmutableTrackingMultimap} that provides a window to read
     * through to the {@code source} but prevents additional writes.
     * 
     * @param source
     * @return an immutable view of the {@code source}
     */
    public static <K, V> ImmutableTrackingMultimap<K, V> of(
            TrackingMultimap<K, V> source) {
        return new ImmutableTrackingMultimap<>(source);
    }

    /**
     * The source {@link TrackingMultimap} that is made immutable by this
     * wrapper.
     */
    private final TrackingMultimap<K, V> source;

    /**
     * Construct a new instance.
     * 
     * @param delegate
     * @param comparator
     */
    protected ImmutableTrackingMultimap(TrackingMultimap<K, V> source) {
        super(Collections.emptyMap(), null);
        this.source = source;
    }

    @Override
    @Deprecated
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsDataType(DataType type) {
        return source.containsDataType(type);
    }

    @Override
    public boolean containsKey(Object key) {
        return source.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return source.containsValue(value);
    }

    @Override
    public long count() {
        return source.count();
    }

    @Override
    @Deprecated
    public boolean delete(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double distinctiveness() {
        return source.distinctiveness();
    }

    @Override
    public Set<Entry<K, Set<V>>> entrySet() {
        return source.entrySet();
    }

    @Override
    public boolean equals(Object obj) {
        return source.equals(obj);
    }

    @Override
    public Set<V> get(Object key) {
        return source.get(key);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    @Override
    public boolean hasValue(V value) {
        return source.hasValue(value);
    }

    @Override
    @Deprecated
    public boolean insert(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return source.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return source.keySet();
    }

    @Override
    public K max() {
        return source.max();
    }

    @Override
    @Deprecated
    public Set<V> merge(K key, Set<V> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K min() {
        return source.min();
    }

    @Override
    public double percentKeyDataType(DataType type) {
        return source.percentKeyDataType(type);
    }

    @Override
    public double proportion(K element) {
        return source.proportion(element);
    }

    @Override
    @Deprecated
    public Set<V> put(K key, Set<V> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public void putAll(Map<? extends K, ? extends Set<V>> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public Set<V> remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return source.size();
    }

    @Override
    public double spread() {
        return source.spread();
    }

    @Override
    public String toString() {
        return source.toString();
    }

    @Override
    public double uniqueness() {
        return source.uniqueness();
    }

    @Override
    public Collection<Set<V>> values() {
        return source.values();
    }

    @Override
    public VariableType variableType() {
        return source.variableType();
    }

    @Override
    @Deprecated
    protected Object clone() throws CloneNotSupportedException {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    protected Set<V> createValueSet() {
        throw new UnsupportedOperationException();
    }

}
