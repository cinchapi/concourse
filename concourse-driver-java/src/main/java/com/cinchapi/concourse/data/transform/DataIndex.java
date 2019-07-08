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
package com.cinchapi.concourse.data.transform;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.cinchapi.concourse.data.Index;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;
import com.cinchapi.concourse.util.Transformers;

/**
 * Synthetic class that represents a result set that maps records to keys to
 * values.
 *
 * @author Jeff Nelson
 */
public final class DataIndex<T> extends AbstractMap<String, Map<T, Set<Long>>>
        implements Index<T> {

    /**
     * Convert the {@link TObject} values in the {@code results} to their java
     * counterparts and {@link PrettyLinkedTableMap prettify} the result set
     * lazily.
     * 
     * @param data
     * @return the converted {@code results} in the form of a {@link Map} whose
     *         {@link #toString()} method does pretty printing
     */
    public static <T> Map<String, Map<T, Set<Long>>> of(
            Map<String, Map<TObject, Set<Long>>> data) {
        return new DataIndex<>(data);
    }

    /**
     * The data that must be transformed.
     */
    private final Map<String, Map<TObject, Set<Long>>> data;

    /**
     * A cache of the prettified results
     */
    private Map<String, Map<T, Set<Long>>> pretty = null;

    /**
     * A cache of the transformed, but unpretty results.
     */
    private Map<String, Map<T, Set<Long>>> transformed = null;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private DataIndex(Map<String, Map<TObject, Set<Long>>> data) {
        this.data = data;
    }

    @Override
    public Set<Entry<String, Map<T, Set<Long>>>> entrySet() {
        if(transformed == null) {
            transformed = data.entrySet().stream().map(entry -> {
                return new SimpleImmutableEntry<>(entry.getKey(),
                        Transformers.<TObject, T, Long, Long> transformMapSet(
                                entry.getValue(),
                                Conversions.thriftToJavaCasted(),
                                Conversions.none()));
            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                    (e1, e2) -> e2, LinkedHashMap::new));
        }
        return transformed.entrySet();
    }

    @Override
    public String toString() {
        if(pretty == null) {
            Map<String, Map<T, Set<Long>>> $pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Key");
            entrySet().forEach(
                    entry -> $pretty.put(entry.getKey(), entry.getValue()));
            pretty = $pretty;
            transformed = pretty;
        }
        return pretty.toString();
    }

}
