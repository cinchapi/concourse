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
package com.cinchapi.concourse.data.sort;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.EmptyOperationException;
import com.cinchapi.concourse.data.Column;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Column} whose contents can be sorted using a {@link Sorter}.
 * <p>
 * In practice, a {@link SortableColumn} simply forwards to another
 * {@link Column}. When {@link #sort(Sorter)} is called, the delegate is
 * replaced by a sorted version.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class SortableColumn<V> extends ForwardingMap<Long, V> implements
        Column<V>,
        Sortable<V> {

    /**
     * Ensure that the {@code data} is a {@link SortableColumn}.
     * 
     * @param key
     * @param data
     * @return the {@code data} in the form of a {@link SortableColumn}.
     */
    public static <V> SortableColumn<Set<V>> multiValued(String key,
            Map<Long, Set<V>> data) {
        if(data instanceof SortableColumn) {
            return (SortableColumn<Set<V>>) data;
        }
        else {
            return new SortableColumn<>(key, data);
        }
    }

    /**
     * Ensure that the {@code data} is a {@link SortableColumn}.
     * 
     * @param key
     * @param data
     * @return the {@code data} in the form of a {@link SortableColumn}.
     */
    public static <V> SortableColumn<V> singleValued(String key,
            Map<Long, V> data) {
        if(data instanceof SortableColumn) {
            return (SortableColumn<V>) data;
        }
        else {
            return new SortableColumn<>(key, data);
        }

    }

    /**
     * The delegate to which calls are forwarded. If {@link #sort(Sorter)}
     * has been called, this delegate is *eventually* sorted.
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
    private SortableColumn(String key, Map<Long, V> delegate) {
        this.delegate = delegate;
        this.key = key;
    }

    @Override
    public void sort(Sorter<V> sorter) {
        try {
            delegate = sorter.sort(sortable())
                    .collect(Collectors.toMap(Entry::getKey,
                            entry -> entry.getValue().get(this.key),
                            (e1, e2) -> e2, LinkedHashMap::new));
        }
        catch (EmptyOperationException e) {}
    }

    @Override
    public void sort(Sorter<V> sorter, long at) {
        try {
            delegate = sorter.sort(sortable(), at)
                    .collect(Collectors.toMap(Entry::getKey,
                            entry -> entry.getValue().get(this.key),
                            (e1, e2) -> e2, LinkedHashMap::new));
        }
        catch (EmptyOperationException e) {}
    }

    @Override
    protected Map<Long, V> delegate() {
        return delegate;
    }

    /**
     * Return a "sortable" view of this column (e.g. pin {@link #key} as the
     * only key/column in a {@link SortableTable}.
     * 
     * @return a sortable view of the {@link #delegate}
     */
    private Stream<Entry<Long, Map<String, V>>> sortable() {
        return delegate.entrySet().stream()
                .map(entry -> new SimpleImmutableEntry<>(entry.getKey(),
                        ImmutableMap.of(this.key, entry.getValue())));
    }

}
