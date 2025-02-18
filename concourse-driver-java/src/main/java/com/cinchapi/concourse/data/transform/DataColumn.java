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
package com.cinchapi.concourse.data.transform;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.concourse.data.Column;
import com.cinchapi.concourse.data.Row;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Conversions;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.PrettyLinkedHashMap;

/**
 * A {@link Row} based on a {@link TObject} result set, that transforms
 * values on the fly.
 *
 * @author Jeff Nelson
 */
public abstract class DataColumn<F, T>
        extends PrettyTransformMap<Long, Long, F, T> implements
        Column<T> {

    /**
     * Return a {@link DataColumn} that contains multi-valued cells of
     * {@link TObject} values that are converted to their Java equivalents.
     * 
     * @param key
     * @param data
     * @return the {@link DataColumn}
     */
    public static <T> DataColumn<Set<TObject>, Set<T>> multiValued(String key,
            Map<Long, Set<TObject>> data) {
        return new MultiValuedColumn<>(key, data);
    }

    /**
     * Return a {@link DataColumn} that contains single-valued cells of
     * {@link TObject} values that are converted to their Java equivalents.
     * 
     * @param key
     * @param data
     * @return the {@link DataColumn}
     */
    public static <T> DataColumn<TObject, T> singleValued(String key,
            Map<Long, TObject> data) {
        return new SingleValuedColumn<>(key, data);
    }

    /**
     * The key to which values are implicitly associated.
     */
    private final String key;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    private DataColumn(String key, Map<Long, F> data) {
        super(data);
        this.key = key;
    }

    @Override
    protected Supplier<Map<Long, T>> $prettyMapSupplier() {
        return () -> PrettyLinkedHashMap.create("Record", key);
    }

    @Override
    protected Long transformKey(Long key) {
        return key;
    }

    /**
     * A {@link DataColumn} for multi-valued cells.
     *
     * @author Jeff Nelson
     */
    private static class MultiValuedColumn<T>
            extends DataColumn<Set<TObject>, Set<T>> {

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param data
         */
        protected MultiValuedColumn(String key, Map<Long, Set<TObject>> data) {
            super(key, data);
        }

        @Override
        protected Set<T> transformValue(Set<TObject> value) {
            return LazyTransformSet.of(value, Conversions.thriftToJavaCasted());
        }

    }

    /**
     * A {@link DataColumn} for single-valued cells.
     *
     * @author Jeff Nelson
     */
    private static class SingleValuedColumn<T> extends DataColumn<TObject, T> {

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param data
         */
        protected SingleValuedColumn(String key, Map<Long, TObject> data) {
            super(key, data);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected T transformValue(TObject value) {
            return (T) Convert.thriftToJava(value);
        }

    }

}
