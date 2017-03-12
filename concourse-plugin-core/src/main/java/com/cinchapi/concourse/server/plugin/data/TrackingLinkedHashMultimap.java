/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.util.Map;
import java.util.Set;

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
        return new TrackingLinkedHashMultimap<K, V>(
                Maps.<K, Set<V>> newLinkedHashMap());
    }

    /**
     * Construct a new instance.
     * 
     * @param delegate
     */
    protected TrackingLinkedHashMultimap(Map<K, Set<V>> delegate) {
        super(delegate);
    }

    @Override
    protected Set<V> createValueSet() {
        return Sets.newLinkedHashSet();
    }

}
