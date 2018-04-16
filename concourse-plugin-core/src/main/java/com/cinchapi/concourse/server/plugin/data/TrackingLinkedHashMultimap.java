/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@link TrackingMultimap} that stores items in insertion-order.
 * <p>
 * Internally, this map delegates to a {@link LinkedHashMap} to maintain
 * insertion-order iteration. As such, this map is mostly appropriate for
 * read-only collections that are, perhaps, copies of existing maps that have
 * already been properly sorted.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class TrackingLinkedHashMultimap<K, V> extends TrackingMultimap<K, V> {

    /**
     * Return a new and empty {@link TrackingLinkedHashMultimap}.
     * 
     * @return the map
     */
    public static <K, V> TrackingLinkedHashMultimap<K, V> create() {
        return create(null);
    }

    /**
     * Return a new and empty {@link TrackingLinkedHashMultimap} that supports
     * some features of the {@link SortedMap} interface by using the specified
     * key {@code comparator}
     * 
     * @param comparator used to sort keys
     * @return the map
     */
    public static <K, V> TrackingLinkedHashMultimap<K, V> create(
            @Nullable Comparator<K> comparator) {
        return new TrackingLinkedHashMultimap<K, V>(
                Maps.<K, Set<V>> newLinkedHashMap(), comparator);
    }

    /**
     * Construct a new instance.
     * 
     * @param delegate
     * @param comparator
     */
    protected TrackingLinkedHashMultimap(Map<K, Set<V>> delegate,
            @Nullable Comparator<K> comparator) {
        super(delegate, comparator);
    }

    @Override
    protected Set<V> createValueSet() {
        return Sets.newLinkedHashSet();
    }

}
