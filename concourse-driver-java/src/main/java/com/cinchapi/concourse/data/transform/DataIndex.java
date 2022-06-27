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
public final class DataIndex<T> extends
        PrettyTransformMap<String, String, Map<TObject, Set<Long>>, Map<T, Set<Long>>>
        implements
        Index<T> {

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
     * Construct a new instance.
     * 
     * @param data
     */
    private DataIndex(Map<String, Map<TObject, Set<Long>>> data) {
        super(data);
    }

    @Override
    protected Supplier<Map<String, Map<T, Set<Long>>>> $prettyMapSupplier() {
        return () -> PrettyLinkedTableMap.create("Key");
    }


    @Override
    protected String transformKey(String key) {
        return key;
    }

    @Override
    protected Map<T, Set<Long>> transformValue(Map<TObject, Set<Long>> value) {
        return Transformers.<TObject, T, Long, Long> transformMapSet(value,
                Conversions.thriftToJavaCasted(), Conversions.none());
    }

}
