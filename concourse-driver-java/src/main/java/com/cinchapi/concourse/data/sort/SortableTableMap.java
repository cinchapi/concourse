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
package com.cinchapi.concourse.data.sort;

import java.util.Map;

import com.google.common.collect.ForwardingMap;

/**
 * <p>
 * A <strong>TableMap</strong> is a two dimensional mapping from
 * {@link Long} and {@link String} to a value (e.g. Map&lt;Long, Map&lt;String,
 * V&gt;&gt;), similar to a {@link com.google.common.collect.Table}.
 * </p>
 * <p>
 * A {@link SortableTableMap} is one that can be {@link #sort(Sorter)
 * sorted} using a {@link Sorter}.
 * </p>
 * <p>
 * In practice, a {@link SortableTableMap} simply forwards to another map.
 * When {@link #sort(Sorter)} is called, the delegate is replaced by a
 * sorted version.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface SortableTableMap<V>
        extends Sortable<V>, Map<Long, Map<String, V>> {

    /**
     * Ensure that the {@code data} is a {@link SortableTableMap}.
     * 
     * @param data
     * @return the {@code data} in the form of a {@link SortableTableMap}.
     */
    public static <V> SortableTableMap<V> ensure(
            Map<Long, Map<String, V>> data) {
        if(data instanceof SortableTableMap) {
            return (SortableTableMap<V>) data;
        }
        else {
            return new SortableForwardingTableMap<>(data);
        }
    }

    /**
     * An implementation of {@link SortableTableMap} that forwards to another
     * map.
     *
     * @author Jeff Nelson
     */
    class SortableForwardingTableMap<V> extends
            ForwardingMap<Long, Map<String, V>> implements SortableTableMap<V> {

        /**
         * The delegate to which calls are forwarded. If
         * {@link #sort(Sorter)}
         * has
         * been called, this delegate is sorted.
         */
        private Map<Long, Map<String, V>> delegate;

        /**
         * Construct a new instance.
         * 
         * @param map
         */
        private SortableForwardingTableMap(Map<Long, Map<String, V>> map) {
            this.delegate = map;
        }

        @Override
        public void sort(Sorter<V> sorter) {
            delegate = sorter.sort(delegate);
        }

        @Override
        protected Map<Long, Map<String, V>> delegate() {
            return delegate;
        }

    }

}
