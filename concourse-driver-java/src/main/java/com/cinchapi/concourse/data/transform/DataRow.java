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
import com.cinchapi.concourse.data.Row;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;

/**
 * A {@link Row} based on a {@link TObject} result set, that transforms values
 * on the fly.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class DataRow<F, T> extends AbstractMap<String, T> implements
        Row<T> {

    /**
     * Convert the {@link TObject} values in the {@code results} to their java
     * counterparts and {@link PrettyLinkedHashMap prettify} the result set
     * lazily.
     * 
     * @param results
     * @return the converted {@code results} in the form of a {@link Map} whose
     *         {@link #toString()} method does pretty printing
     */
    public static <T> DataRow<Set<TObject>, Set<T>> multiValued(
            Map<String, Set<TObject>> data) {
        return new MultiValuedRow<>(data);
    }

    /**
     * Return a {@link DataRow} that contains single value cells
     * {@link TObject} values that are converted to their Java equivalents.
     * 
     * @param data
     * @return the {@link DataTable}
     */
    public static <T> DataRow<TObject, T> singleValued(
            Map<String, TObject> data) {
        return new SingleValuedRow<>(data);
    }

    /**
     * The data that must be transformed.
     */
    private final Map<String, F> data;

    /**
     * A cache of the prettified results
     */
    private Map<String, T> pretty = null;

    /**
     * A cache of the transformed, but unpretty results.
     */
    private Map<String, T> transformed = null;

    /**
     * The header to use for the column that contains the values.
     */
    private final String valueColumnHeader;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private DataRow(Map<String, F> data, String valueColumnHeader) {
        this.data = data;
        this.valueColumnHeader = valueColumnHeader;
    }

    @Override
    public Set<Entry<String, T>> entrySet() {
        if(transformed == null) {
            transformed = data.entrySet().stream().map(entry -> {
                String key = entry.getKey();
                T value = transform(entry.getValue());
                return new SimpleImmutableEntry<>(key, value);
            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                    (e1, e2) -> e2, LinkedHashMap::new));
        }
        return transformed.entrySet();
    }

    @Override
    public String toString() {
        if(pretty == null) {
            Map<String, T> $pretty = PrettyLinkedHashMap.create("Key",
                    valueColumnHeader);
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
     * A {@link DataRow} for multi-valued cells.
     *
     * @author Jeff Nelson
     */
    private static class MultiValuedRow<T>
            extends DataRow<Set<TObject>, Set<T>> {

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        protected MultiValuedRow(Map<String, Set<TObject>> data) {
            super(data, "Values");
        }

        @Override
        protected Set<T> transform(Set<TObject> value) {
            return LazyTransformSet.of(value, Conversions.thriftToJavaCasted());
        }

    }

    /**
     * A {@link DataRow} for single-valued cells.
     *
     * @author Jeff Nelson
     */
    private static class SingleValuedRow<T> extends DataRow<TObject, T> {

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        protected SingleValuedRow(Map<String, TObject> data) {
            super(data, "Value");
        }

        @SuppressWarnings("unchecked")
        @Override
        protected T transform(TObject value) {
            return (T) Convert.thriftToJava(value);
        }

    }

}
