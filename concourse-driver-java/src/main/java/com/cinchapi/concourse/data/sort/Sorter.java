/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Sorter} imposes an {@link Order} by value on a result set.
 *
 * @author Jeff Nelson
 */
public interface Sorter<V> {

    /**
     * Sort {@code data}.
     * 
     * @param data
     * @return a {@link Stream} that contains each of the entries in
     *         {@code data} in sorted order
     */
    public default Stream<Entry<Long, Map<String, V>>> sort(
            Map<Long, Map<String, V>> data) {
        return sort(data.entrySet().stream());
    }

    /**
     * Sort {@code data} using the {@code at} timestamp as temporal binding for
     * missing value lookups when an order component does not explicitly specify
     * a timestamp.
     * 
     * @param data
     * @param at
     * @return a {@link Stream} that contains each of the entries in
     *         {@code data} in sorted order
     */
    public default Stream<Entry<Long, Map<String, V>>> sort(
            Map<Long, Map<String, V>> data, @Nullable Long at) {
        return sort(data.entrySet().stream(), at);
    }

    /**
     * Sort the {@code stream}.
     * 
     * @param stream
     * @return a {@link Stream} that contains each of the entries in
     *         {@code data} in sorted order
     */
    public Stream<Entry<Long, Map<String, V>>> sort(
            Stream<Entry<Long, Map<String, V>>> stream);

    /**
     * Sort the {@code stream} using the {@code at} timestamp as temporal
     * binding for missing value lookups when an order component does not
     * explicitly specify a timestamp.
     * 
     * @param stream
     * @param at
     * @return a {@link Stream} that contains each of the entries in
     *         {@code data} in sorted order
     */
    public Stream<Entry<Long, Map<String, V>>> sort(
            Stream<Entry<Long, Map<String, V>>> stream, @Nullable Long at);

    /**
     * Sort and collect {@code data}.
     * 
     * @param data
     * @return a collected {@link Map} that contains the {@code data} in sorted
     *         order
     */
    default Map<Long, Map<String, V>> organize(Map<Long, Map<String, V>> data) {
        return sort(data)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (a, b) -> b, () -> new LinkedHashMap<>(data.size())));

    }

    /**
     * Sort and collected {@code data} using the {@code at} timestamp as
     * temporal binding for missing value lookups when an order component does
     * not explicitly specify a timestamp.
     * 
     * @param data
     * @param at
     * @return a collected {@link Map} that contains the {@code data} in sorted
     *         order
     */
    default Map<Long, Map<String, V>> organize(Map<Long, Map<String, V>> data,
            @Nullable Long at) {
        return sort(data, at)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (a, b) -> b, () -> new LinkedHashMap<>(data.size())));
    }

    /**
     * Sort and collect the {@code stream}.
     * 
     * @param data
     * @return a collected {@link Map} that contains the {@code data} in sorted
     *         order
     */
    default Map<Long, Map<String, V>> organize(
            Stream<Entry<Long, Map<String, V>>> stream) {
        return sort(stream).collect(Collectors.toMap(Entry::getKey,
                Entry::getValue, (a, b) -> b, LinkedHashMap::new));
    }

    /**
     * Sort and collected {@code data} using the {@code at} timestamp as
     * temporal binding for missing value lookups when an order component does
     * not explicitly specify a timestamp.
     * 
     * @param data
     * @param at
     * @return a collected {@link Map} that contains the {@code data} in sorted
     *         order
     */
    default Map<Long, Map<String, V>> organize(
            Stream<Entry<Long, Map<String, V>>> stream, @Nullable Long at) {
        return sort(stream, at).collect(Collectors.toMap(Entry::getKey,
                Entry::getValue, (a, b) -> b, LinkedHashMap::new));
    }

}
