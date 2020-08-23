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
import java.util.Set;

import com.cinchapi.concourse.data.Table;
import com.google.common.collect.ForwardingMap;

/**
 * A {@link Table} that can be {@link #sort(Sorter) sorted} using a
 * {@link Sorter}.
 *
 * @author Jeff Nelson
 */
public interface SortableTable<V> extends Sortable<V>, Table<V> {

    /**
     * Ensure that the {@code data} is a {@link SortableTable}.
     * 
     * @param data
     * @return the {@code data} in the form of a {@link SortableTable}.
     */
    public static <V> SortableTable<Set<V>> multiValued(
            Map<Long, Map<String, Set<V>>> data) {
        if(data instanceof SortableTable) {
            return (SortableTable<Set<V>>) data;
        }
        else {
            return new ForwardingSortableTable<>(data);
        }
    }

    /**
     * Ensure that the {@code data} is a {@link SortableTable}.
     * 
     * @param data
     * @return the {@code data} in the form of a {@link SortableTable}.
     */
    public static <V> SortableTable<V> singleValued(
            Map<Long, Map<String, V>> data) {
        if(data instanceof SortableTable) {
            return (SortableTable<V>) data;
        }
        else {
            return new ForwardingSortableTable<>(data);
        }
    }

    /**
     * A {@link SortableTable} that forwards to another delegate and lazily
     * sorts upon request.
     *
     * @author Jeff Nelson
     */
    static class ForwardingSortableTable<V>
            extends ForwardingMap<Long, Map<String, V>> implements
            SortableTable<V> {

        /**
         * The delegate to which calls are forwarded. If {@link #sort(Sorter)}
         * has been called, this delegate is sorted.
         */
        private Map<Long, Map<String, V>> delegate;

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        private ForwardingSortableTable(Map<Long, Map<String, V>> data) {
            this.delegate = data;
        }

        @Override
        public void sort(Sorter<V> sorter) {
            delegate = sorter.sort(delegate());

        }

        @Override
        public void sort(Sorter<V> sorter, long at) {
            delegate = sorter.sort(delegate(), at);
        }

        @Override
        protected Map<Long, Map<String, V>> delegate() {
            return delegate;
        }
    }

}
