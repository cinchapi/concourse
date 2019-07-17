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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Set} whose contents can be sorted using a {@link Sorter}
 *
 * @author Jeff Nelson
 */
public class SortableSet<V> extends ForwardingSet<Long> implements Sortable<V> {

    /**
     * Return a {@link SortableSet}.
     * 
     * @param data
     * @return the {@link SortableSet}
     */
    public static <V> SortableSet<V> of(Set<Long> data) {
        return new SortableSet<>(data);
    }

    /**
     * The delegate to which calls are forwarded. If {@link #sort(Sorter)}
     * has been called, this delegate is sorted.
     */
    private Set<Long> delegate;

    /**
     * Construct a new instance.
     * 
     * @param delegate
     */
    private SortableSet(Set<Long> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void sort(Sorter<V> sorter) {
        Map<Long, Map<String, V>> sorted = sorter.sort(sortable());
        delegate = sorted.entrySet().stream().map(entry -> {
            return entry.getKey();
        }).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public void sort(Sorter<V> sorter, long at) {
        Map<Long, Map<String, V>> sorted = sorter.sort(sortable(), at);
        delegate = sorted.entrySet().stream().map(entry -> {
            return entry.getKey();
        }).collect(Collectors.toCollection(LinkedHashSet::new));

    }

    /**
     * Return a "sortable" view of this column (e.g. pin {@link #key} as the
     * only key/column in a {@link SortableTable}.
     * 
     * @return a sortable view of the {@link #delegate}
     */
    private Map<Long, Map<String, V>> sortable() {
        return delegate.stream().map(record -> {
            Long key = record;
            Map<String, V> value = ImmutableMap.of();
            return new SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    protected Set<Long> delegate() {
        return delegate;
    }

}
