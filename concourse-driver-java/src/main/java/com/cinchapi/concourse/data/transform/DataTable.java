/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.concourse.data.Table;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedTableMap;

/**
 * A result {@link Table} that lazily transforms values from one type to another
 * and prettifies the string representation of the data.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class DataTable<F, T>
        extends PrettyTransformMap<Long, Long, Map<String, F>, Map<String, T>>
        implements
        Table<T> {

    /**
     * Return a {@link DataTable} that contains multi-valued cells of
     * {@link TObject} values that are converted to their Java equivalents.
     * 
     * @param data
     * @return the {@link DataTable}
     */
    public static <T> DataTable<Set<TObject>, Set<T>> multiValued(
            Map<Long, Map<String, Set<TObject>>> data) {
        return new MultiValuedTable<>(data);
    }

    /**
     * Return a {@link DataTable} that contains single value cells
     * {@link TObject} values that are converted to their Java equivalents.
     * 
     * @param data
     * @return the {@link DataTable}
     */
    public static <T> DataTable<TObject, T> singleValued(
            Map<Long, Map<String, TObject>> data) {
        return new SingleValuedTable<>(data);
    }

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    protected DataTable(Map<Long, Map<String, F>> data) {
        super(data);
    }

    @Override
    protected Supplier<Map<Long, Map<String, T>>> $prettyMapSupplier() {
        return () -> PrettyLinkedTableMap.create("Record");
    }

    @Override
    protected Long transformKey(Long key) {
        return key;
    }

    /**
     * A {@link DataTable} for multi-valued cells.
     *
     * @author Jeff Nelson
     */
    private static class MultiValuedTable<T>
            extends DataTable<Set<TObject>, Set<T>> {

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        protected MultiValuedTable(Map<Long, Map<String, Set<TObject>>> data) {
            super(data);
        }

        @Override
        protected Map<String, Set<T>> transformValue(
                Map<String, Set<TObject>> value) {
            return value.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey,
                            entry -> LazyTransformSet.of(entry.getValue(),
                                    Conversions.thriftToJavaCasted()),
                            (a, b) -> b,
                            () -> new LinkedHashMap<>(value.size())));
        }

    }

    /**
     * A {@link DataTable} for single-valued cells.
     *
     * @author Jeff Nelson
     */
    private static class SingleValuedTable<T> extends DataTable<TObject, T> {

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        protected SingleValuedTable(Map<Long, Map<String, TObject>> data) {
            super(data);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Map<String, T> transformValue(Map<String, TObject> value) {
            return value.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey,
                            entry -> (T) Convert.thriftToJava(entry.getValue()),
                            (a, b) -> b,
                            () -> new LinkedHashMap<>(value.size())));
        }

    }

}
