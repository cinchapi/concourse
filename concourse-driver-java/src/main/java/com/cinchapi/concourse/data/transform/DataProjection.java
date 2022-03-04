/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import com.cinchapi.concourse.data.Projection;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;

/**
 * A {@link Projection} based on a {@link TObject} result set, that transforms
 * values on the fly.
 *
 * @author Jeff Nelson
 */
public class DataProjection<T> extends AbstractMap<T, Set<Long>> implements
        Projection<T> {

    /**
     * Convert the {@link TObject} values in the {@code results} to their java
     * counterparts and {@link PrettyLinkedHashMap prettify} the result set
     * lazily.
     * 
     * @param data
     * @return the converted {@code results} in the form of a {@link Map} whose
     *         {@link #toString()} method does pretty printing
     */
    public static <T> DataProjection<T> of(Map<TObject, Set<Long>> data) {
        return new DataProjection<>(data);
    }

    /**
     * The data that must be transformed.
     */
    private final Map<TObject, Set<Long>> data;

    /**
     * A cache of the prettified results
     */
    private Map<T, Set<Long>> pretty = null;

    /**
     * A cache of the transformed, but unpretty results.
     */
    private Map<T, Set<Long>> transformed = null;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private DataProjection(Map<TObject, Set<Long>> data) {
        this.data = data;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Entry<T, Set<Long>>> entrySet() {
        if(transformed == null) {
            transformed = data.entrySet().stream().map(entry -> {
                return new SimpleImmutableEntry<>(
                        (T) Convert.thriftToJava(entry.getKey()),
                        entry.getValue());
            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                    (e1, e2) -> e2, LinkedHashMap::new));
        }
        return transformed.entrySet();
    }

    @Override
    public String toString() {
        if(pretty == null) {
            Map<T, Set<Long>> $pretty = PrettyLinkedHashMap.create("Value",
                    "Records");
            entrySet().forEach(
                    entry -> $pretty.put(entry.getKey(), entry.getValue()));
            pretty = $pretty;
            transformed = pretty;
        }
        return pretty.toString();
    }

}
