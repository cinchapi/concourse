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
package com.cinchapi.concourse.server.query.paginate;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.cinchapi.concourse.lang.paginate.NoPage;
import com.cinchapi.concourse.lang.paginate.Page;
import com.google.common.collect.Iterables;

/**
 * A utility for applying {@link Page Pages}.
 *
 * @author Jeff Nelson
 */
public final class Paging {

    /**
     * Return a {@link Map} that only contains the entries from {@code map} on
     * the {@code page}.
     * 
     * @param map
     * @param page
     * @return a {@link Map} containing the entries from {@code map} on the
     *         {@code page}.
     */
    public static <K, V> Map<K, V> page(Map<K, V> map, Page page) {
        if(page instanceof NoPage
                || (page.skip() == 0 && page.limit() == Integer.MAX_VALUE)) {
            return map;
        }
        else {
            Iterable<Entry<K, V>> paged = $page(map.entrySet(), page);
            return new AbstractMap<K, V>() {

                @Override
                public Set<Entry<K, V>> entrySet() {
                    return new AbstractSet<Entry<K, V>>() {

                        @Override
                        public Iterator<Entry<K, V>> iterator() {
                            return paged.iterator();
                        }

                        @Override
                        public int size() {
                            // TODO: can the size be calced from the original
                            // map size and the page semantics?
                            return Iterables.size(paged);
                        }

                    };
                }

            };
        }
    }

    /**
     * Return an {@link Iterable} that only contains the items from
     * {@code iterable} on the {@code page}.
     * 
     * @param iterable
     * @param page
     * @return an {@link Iterable} containing the items from {@code iterable} on
     *         the {@code page}.
     */
    public static <T> Iterable<T> page(Iterable<T> iterable, Page page) {
        if(page instanceof NoPage
                || (page.skip() == 0 && page.limit() == Integer.MAX_VALUE)) {
            return iterable;
        }
        else {
            return $page(iterable, page);
        }
    }

    /**
     * Return a {@link Set} that only contains the items from
     * {@code iterable} on the {@code page}.
     * 
     * @param iterable
     * @param page
     * @return an {@link Set} containing the items from {@code iterable} on
     *         the {@code page}.
     */
    public static <T> Set<T> page(Set<T> iterable, Page page) {
        if(page instanceof NoPage
                || (page.skip() == 0 && page.limit() == Integer.MAX_VALUE)) {
            return iterable;
        }
        else {
            Iterable<T> paged = $page(iterable, page);
            return new AbstractSet<T>() {

                @Override
                public Iterator<T> iterator() {
                    return paged.iterator();
                }

                @Override
                public int size() {
                    // TODO: can the size be calced from the original
                    // map size and the page semantics?
                    return Iterables.size(paged);
                }

            };
        }
    }

    /**
     * Internal method to limit the items in {@code iterable} to those on
     * {@code page} without performing bounds checks.
     * 
     * @param iterable
     * @param page
     * @return an {@link Iterable} containing the items from {@code iterable} on
     *         the {@code page}.
     */
    private static <T> Iterable<T> $page(Iterable<T> iterable, Page page) {
        return Iterables.limit(Iterables.skip(iterable, page.skip()),
                page.limit());
    }

    private Paging() {/* no-init */}
}
