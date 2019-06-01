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
package com.cinchapi.concourse.data;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
public final class RecordKeyValuesResultSet<T>
        extends AbstractMap<Long, Map<String, Set<T>>> {

    /**
     * Convert the {@link TObject} values in the {@code results} to their java
     * counterparts and {@link PrettyLinkedTableMap prettify} the result set
     * lazily.
     * 
     * @param rowName
     * @param results
     * @return the converted {@code results} in the form of a {@link Map} whose
     *         {@link #toString()} method does pretty printing
     */
    public static <T> Map<Long, Map<String, Set<T>>> of(
            Map<Long, Map<String, Set<TObject>>> results) {

        return new AbstractMap<Long, Map<String, Set<T>>>() {

            /**
             * A cache of the prettified results
             */
            private Map<Long, Map<String, Set<T>>> pretty = null;

            @Override
            public Set<Entry<Long, Map<String, Set<T>>>> entrySet() {
                return pretty != null ? pretty.entrySet()
                        : results.entrySet().stream().map(entry -> {
                            return new SimpleImmutableEntry<>(entry.getKey(),
                                    Transformers.transformMapSet(
                                            entry.getValue(),
                                            Conversions.<String> none(),
                                            Conversions
                                                    .<T> thriftToJavaCasted()));
                        }).collect(Collectors.toSet());
            }

            @Override
            public String toString() {
                if(pretty == null) {
                    Map<Long, Map<String, Set<T>>> $pretty = PrettyLinkedTableMap
                            .newPrettyLinkedTableMap("Record");
                    entrySet().forEach(entry -> $pretty.put(entry.getKey(),
                            entry.getValue()));
                    pretty = $pretty;
                }
                return pretty.toString();
            }

        };

    }

    private RecordKeyValuesResultSet() {/* no-init */}

    @Override
    public Set<Entry<Long, Map<String, Set<T>>>> entrySet() {
        throw new UnsupportedOperationException();
    }

}
