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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.data.sort.Sortable;
import com.cinchapi.concourse.data.sort.Sorter;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;

/**
 * <p>
 * A {@link SortableMap} is one that can be {@link #sort(StoreSorter) sorted} using a
 * {@link StoreSorter}.
 * </p>
 * <p>
 * A {@link SortableMap} is implicitly a {@link SortableTableMap} with a single
 * key/column.
 * </p>
 * <p>
 * In practice, a {@link SortableMap} simply forwards to another map. When
 * {@link #sort(StoreSorter)} is called, the delegate is replaced by a sorted
 * version.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class SortableMap<V> extends ForwardingMap<Long, V> implements
        Sortable<V> {

    /**
     * The delegate to which calls are forwarded. If {@link #sort(StoreSorter)} has
     * been called, this delegate is sorted.
     */
    private Map<Long, V> delegate;

    /**
     * The key associated with each value.
     */
    private final String key;

    /**
     * Construct a new instance.
     * 
     * @param map
     */
    protected SortableMap(String key, Map<Long, V> delegate) {
        this.delegate = delegate;
        this.key = key;
    }

    @Override
    public void sort(Sorter<V> sorter) {
        Map<Long, Map<String, V>> sorted = sorter.sort(sortable());
        delegate = sorted.entrySet().stream().map(entry -> {
            Long key = entry.getKey();
            V value = entry.getValue().get(this.key);
            return new SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * Return a "sortable" view of this map (e.g. pin {@link #key} as the only
     * key/column in a {@link SortableTableMap}.
     * 
     * @return a sortable view of the {@link #delegate}
     */
    private Map<Long, Map<String, V>> sortable() {
        return delegate.entrySet().stream().map(entry -> {
            Long key = entry.getKey();
            Map<String, V> value = ImmutableMap.of(this.key, entry.getValue());
            return new SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    protected Map<Long, V> delegate() {
        return delegate;
    }

}
