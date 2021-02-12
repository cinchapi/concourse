/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
public abstract class DataTable<F, T> extends AbstractMap<Long, Map<String, T>>
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
     * The data that must be transformed.
     */
    private final Map<Long, Map<String, F>> data;

    /**
     * A cache of the transformed and prettified results
     */
    private Map<Long, Map<String, T>> pretty = null;

    /**
     * A cache of the transformed, but unpretty results.
     */
    private Map<Long, Map<String, T>> transformed = null;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    protected DataTable(Map<Long, Map<String, F>> data) {
        this.data = data;
    }

    @Override
    public Set<Entry<Long, Map<String, T>>> entrySet() {
        if(transformed == null) {
            transformed = data.entrySet().stream().map(entry -> {
                Long key = entry.getKey();
                Map<String, T> value = entry.getValue().entrySet().stream()
                        .map(e -> {
                            return new SimpleImmutableEntry<>(e.getKey(),
                                    transform(e.getValue()));
                        }).collect(
                                Collectors.toMap(Entry::getKey, Entry::getValue,
                                        (e1, e2) -> e2, LinkedHashMap::new));
                return new SimpleImmutableEntry<>(key, value);
            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                    (e1, e2) -> e2, LinkedHashMap::new));
        }
        return transformed.entrySet();
    }

    @Override
    public String toString() {
        if(pretty == null) {
            Map<Long, Map<String, T>> $pretty = PrettyLinkedTableMap
                    .newPrettyLinkedTableMap("Record");
            entrySet().forEach(
                    entry -> $pretty.put(entry.getKey(), entry.getValue()));
            pretty = $pretty;
            transformed = pretty;
        }
        return pretty.toString();
    }

    /**
     * Transform the {@code value} to the appropriate type.
     * 
     * @param value
     * @return the transformed value.
     */
    protected abstract T transform(F value);

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
        protected Set<T> transform(Set<TObject> value) {
            return LazyTransformSet.of(value, Conversions.thriftToJavaCasted());
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
        protected T transform(TObject value) {
            return (T) Convert.thriftToJava(value);
        }

    }

}
