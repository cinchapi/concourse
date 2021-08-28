/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.io.Byteable;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Record} that does not remember its history and therefore can only
 * respond to present state queries.
 *
 * @author Jeff Nelson
 */
public abstract class AmnesiaRecord<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends Record<L, K, V> {

    /**
     * Tracks the {@link #cardinality()}.
     */
    private final AtomicInteger cardinality = new AtomicInteger(0);

    /**
     * Construct a new instance.
     * 
     * @param locator
     * @param key
     */
    protected AmnesiaRecord(L locator, @Nullable K key) {
        super(locator, key);
    }

    @Override
    public int cardinality() {
        return cardinality.get();
    }

    @Override
    public final boolean contains(K key, V value, long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Set<V> get(K key, long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Map<K, Set<V>> getAll(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Set<K> keys(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected final Map<K, List<CompactRevision<V>>> $createHistoryMap() {
        return new NoOpHistoryMap();
    }

    @Override
    protected void onAppend(Revision<L, K, V> revision) {
        cardinality.incrementAndGet();
        super.onAppend(revision);
    }

    /**
     * A {@link Map} that ignores attempts of the parent class to record
     * historical {@link Revision revisions}.
     *
     * @author Jeff Nelson
     */
    private class NoOpHistoryMap
            extends AbstractMap<K, List<CompactRevision<V>>> {

        /**
         * A {@link List} that ignores attempts of the parent class to record
         * historical {@link Revision Revisions}. This is always returned from
         * {@link #get(Object)}.
         */
        private final List<CompactRevision<V>> noOpList = new AbstractList<CompactRevision<V>>() {

            @Override
            public boolean add(CompactRevision<V> e) {
                return false;
            }

            @Override
            public CompactRevision<V> get(int index) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }

        };

        @Override
        public Set<Entry<K, List<CompactRevision<V>>>> entrySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public List<CompactRevision<V>> get(Object key) {
            return noOpList;
        }

        @Override
        public int hashCode() {
            return ImmutableMap.of().hashCode();
        }

        @Override
        public List<CompactRevision<V>> put(K key,
                List<CompactRevision<V>> value) {
            return null;
        }

        @Override
        public int size() {
            return cardinality.get();
        }

        @Override
        public String toString() {
            return "[]";
        }

    }

}
