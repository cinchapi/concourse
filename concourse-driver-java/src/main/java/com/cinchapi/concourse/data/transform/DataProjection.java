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

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

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
public class DataProjection<T>
        extends PrettyTransformMap<TObject, T, Set<Long>, Set<Long>> implements
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
     * Construct a new instance.
     * 
     * @param data
     */
    private DataProjection(Map<TObject, Set<Long>> data) {
        super(data);
    }

    @Override
    protected Supplier<Map<T, Set<Long>>> $prettyMapSupplier() {
        return () -> PrettyLinkedHashMap.create("Value", "Records");
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T transformKey(TObject key) {
        return (T) Convert.thriftToJava(key);
    }

    @Override
    protected Set<Long> transformValue(Set<Long> value) {
        return value;
    }

}
