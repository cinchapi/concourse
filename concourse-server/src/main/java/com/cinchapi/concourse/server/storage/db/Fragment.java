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
package com.cinchapi.concourse.server.storage.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.server.io.Byteable;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Fragment} represents a subset of another {@link Record}.
 * <p>
 * {@link Fragment Fragments} are used to asynchronously read {@link Revision
 * revisions} into memory to later be {@link Record#append(Fragment...) merged}
 * into a complete {@link Record}.
 * </p>
 * <p>
 * Regardless of their locator or key type, {@link Fragment Fragments} are
 * optimized for O(1) accumulation of {@link Revision revisions} in a
 * <strong>single thread</strong> and do not support any query operations.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class Fragment<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends Record<L, K, V> {

    /**
     * The accumulated {@link Revision revisions}.
     */
    private final List<Revision<L, K, V>> revisions = new ArrayList<>();

    /**
     * Construct a new instance.
     * 
     * @param locator
     * @param key
     */
    protected Fragment(L locator, K key) {
        super(locator, key);
    }

    @Override
    public void append(Revision<L, K, V> revision) {
        revisions.add(revision);
    }

    @Override
    public int cardinality() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(K key, V value, long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<V> get(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, Set<V>> getAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, Set<V>> getAll(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return revisions.isEmpty();
    }

    @Override
    public Set<K> keys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keys(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Map<K, Set<V>> $createDataMap() {
        return ImmutableMap.of();
    }

    /**
     * Return the accumulated {@link Revision revisions} in this
     * {@link Fragment}.
     * 
     * @return the {@link Revision revisions}
     */
    List<Revision<L, K, V>> revisions() {
        return revisions;
    }

}
