/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.sort;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import com.cinchapi.concourse.EmptyOperationException;
import com.cinchapi.concourse.data.sort.Sorter;
import com.cinchapi.concourse.lang.sort.NoOrder;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.server.ops.Stores;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.TObject;
import com.google.common.collect.Iterables;

/**
 * Factory methods and utility functions for {@link Sorting}.
 *
 * @author Jeff Nelson
 */
public final class Sorting {

    /**
     * Return a {@link StoreSorter} that sorts a result set containing scalar
     * values.
     * 
     * @param order
     * @param store
     * @return the {@link StoreSorter}
     */
    public static Sorter<TObject> byValue(Order order, Store store) {
        return order instanceof NoOrder ? new NoOrderSorter<>()
                : new ByValueSorter(order, store);
    }

    /**
     * Return a {@link StoreSorter} that sorts a result set containing a set of
     * values.
     * 
     * @param order
     * @param store
     * @return the {@link StoreSorter}
     */
    public static Sorter<Set<TObject>> byValues(Order order, Store store) {
        return order instanceof NoOrder ? new NoOrderSorter<>()
                : new ByValuesSorter(order, store);
    }

    private Sorting() {/* no-init */}

    /**
     * A {@link Sorter} that doesn't actually do any sorting.
     *
     * @author Jeff Nelson
     */
    private static class NoOrderSorter<V> implements Sorter<V> {

        @Override
        public Stream<Entry<Long, Map<String, V>>> sort(
                Stream<Entry<Long, Map<String, V>>> data) {
            throw EmptyOperationException.INSTNACE;
        }

        @Override
        public Stream<Entry<Long, Map<String, V>>> sort(
                Stream<Entry<Long, Map<String, V>>> data, Long at) {
            throw EmptyOperationException.INSTNACE;
        }

    }

    /**
     * A {@link StoreSorter} for scalar values.
     *
     * @author Jeff Nelson
     */
    private static class ByValueSorter extends StoreSorter<TObject> {

        /**
         * Construct a new instance.
         * 
         * @param order
         * @param store
         */
        protected ByValueSorter(Order order, Store store) {
            super(order, store);
        }

        @Override
        protected int compare(TObject v1, TObject v2) {
            return Comparables.nullSafeCompare(v1, v2);
        }

        @Override
        protected TObject lookup(String key, long record) {
            return Iterables.getLast(Stores.select(store, key, record));
        }

        @Override
        protected TObject lookup(String key, long record, long timestamp) {
            return Iterables
                    .getLast(Stores.select(store, key, record, timestamp));
        }

    }

    /**
     * A {@link StoreSorter} for sets of values.
     *
     * @author Jeff Nelson
     */
    private static class ByValuesSorter extends StoreSorter<Set<TObject>> {

        /**
         * Construct a new instance.
         * 
         * @param order
         * @param store
         */
        protected ByValuesSorter(Order order, Store store) {
            super(order, store);
        }

        @Override
        protected int compare(Set<TObject> v1, Set<TObject> v2) {
            return Comparables.compare(v1, v2);
        }

        @Override
        protected Set<TObject> lookup(String key, long record) {
            return Stores.select(store, key, record);
        }

        @Override
        protected Set<TObject> lookup(String key, long record, long timestamp) {
            return Stores.select(store, key, record, timestamp);
        }

    }

}
