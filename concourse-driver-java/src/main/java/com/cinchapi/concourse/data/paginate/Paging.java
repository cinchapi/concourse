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
package com.cinchapi.concourse.data.paginate;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.cinchapi.concourse.lang.paginate.Page;

/**
 * A utility for applying {@link Page Pages}.
 *
 * @author Jeff Nelson
 */
public final class Paging {

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param map
     * @param page
     * @return the paginated data
     */
    public static <K, V> Map<K, V> paginate(Map<K, V> map, Page page) {
        return paginate(map, page, LinkedHashMap::new);
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param map
     * @param page
     * @param supplier
     * @return the paginated data
     */
    public static <K, V, M extends Map<K, V>> M paginate(Map<K, V> map,
            Page page, Supplier<M> supplier) {
        return paginate(map, page, supplier,
                (m, e) -> m.put(e.getKey(), e.getValue()));
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param map
     * @param page
     * @param supplier
     * @param accumulator
     * @return the paginated data
     */
    public static <K, V, M extends Map<K, V>> M paginate(Map<K, V> map,
            Page page, Supplier<M> supplier,
            BiConsumer<Map<K, V>, Entry<K, V>> accumulator) {
        Stream<Entry<K, V>> stream = paginate(map.entrySet().stream(), page);
        M $map = supplier.get();
        stream.forEach(entry -> accumulator.accept($map, entry));
        return $map;
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param set
     * @param page
     * @return the paginated data
     */
    public static <T> Set<T> paginate(Set<T> set, Page page) {
        return paginate(set, page, LinkedHashSet::new);
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param set
     * @param page
     * @param supplier
     * @return the paginated data
     */
    public static <T, S extends Set<T>> S paginate(Set<T> set, Page page,
            Supplier<S> supplier) {
        return paginate(set, page, supplier, (s, i) -> s.add(i));
    }

    /**
     * Return a copy of the original data that only contains data that appears
     * on the provided {@code page}.
     * 
     * @param set
     * @param page
     * @param supplier
     * @param accumulator
     * @return the paginated data
     */
    public static <T, S extends Set<T>> S paginate(Set<T> set, Page page,
            Supplier<S> supplier, BiConsumer<Set<T>, T> accumulator) {
        Stream<T> stream = paginate(set.stream(), page);
        S $set = supplier.get();
        stream.forEach(item -> accumulator.accept($set, item));
        return $set;
    }

    /**
     * Applying the limit/skip of the {@code page} to the {@code stream}.
     * 
     * @param stream
     * @param page
     * @return the modified stream
     */
    public static <T> Stream<T> paginate(Stream<T> stream, Page page) {
        if(page.skip() > 0) {
            stream = stream.skip(page.skip());
        }
        if(page.limit() < Integer.MAX_VALUE) {
            stream = stream.limit(page.limit());
        }
        return stream;
    }

    private Paging() {/* no-init */}

}
