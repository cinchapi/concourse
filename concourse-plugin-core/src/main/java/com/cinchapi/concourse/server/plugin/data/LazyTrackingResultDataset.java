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
package com.cinchapi.concourse.server.plugin.data;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.atomix.catalyst.buffer.Buffer;

/**
 * A {@link ResultDataset} that delays real-time tracking about the nature of
 * the dataset until its required to do so to service relevant operations.
 * <p>
 * This {@link Dataset} forwards operations to internal delegates. Initially and
 * until an operation that requires tracking is called, operations are forward
 * to a copy of the data that does not perform any tracking. This helps to
 * optimize for use cases where data will be added to the {@link Dataset} but
 * tracking isn't needed.
 * </p>
 * <p>
 * When an operation that does require tracking is called, any previously
 * written data is tracked and all subsequent writes will track data in
 * real-time.
 * </p>
 * <p>
 * Additionally, when this {@link Dataset} is {@link Dataset#deserialize(Buffer)
 * deserialized}, it will immediately start tracking. This optimizes for a use
 * case where the {@link Dataset} can be populated efficiently (e.g. without
 * tracking), serialized and then deserialized by another service with tracking
 * enabled.
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class LazyTrackingResultDataset<T> extends ResultDataset<T> {

    /**
     * A copy of the data that does not perform tracking.
     */
    protected Map<Long, Map<String, Set<T>>> data;

    /**
     * A copy of the data that does perform tracking. This is populated in the
     * {@link #tracking()} method.
     */
    protected ResultDataset<T> tracking = null;

    /**
     * A dummy instance of what the {@link #supplier()} returns that is used to
     * hook into the delegate's serialization logic.
     */
    private ResultDataset<T> dummy = null;

    /**
     * Construct a new instance.
     */
    public LazyTrackingResultDataset() {
        this.data = Maps.newLinkedHashMap();
    }

    @Override
    public boolean delete(Long entity, String attribute, T value) {
        return tracking().delete(entity, attribute, value);
    }

    @Override
    public Set<Entry<Long, Map<String, Set<T>>>> entrySet() {
        return data.entrySet();
    }

    @Override
    public boolean equals(Object obj) {
        return data.equals(obj);
    }

    @Override
    public Set<T> get(Long entity, String attribute) {
        return data.getOrDefault(entity, ImmutableMap.of()).get(attribute);
    }

    @Override
    public Map<String, Set<T>> get(Object entity) {
        return data.get(entity);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public boolean insert(Long entity, String attribute, T value) {
        return tracking().insert(entity, attribute, value);
    }

    @Override
    public Map<String, Map<T, Set<Long>>> invert() {
        return tracking().invert();
    }

    @Override
    public Map<T, Set<Long>> invert(String attribute) {
        return tracking().invert(attribute);
    }

    @Override
    public Map<String, Set<T>> put(Long entity, Map<String, Set<T>> mappings) {
        return data.put(entity, mappings);
    }

    @Override
    public Map<String, Set<T>> remove(Object entity) {
        return data.remove(entity);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    protected T deserializeValue(Buffer buffer) {
        return tracking().deserializeValue(buffer);
    }

    @Override
    protected void serializeValue(T value, Buffer buffer) {
        if(dummy == null) {
            dummy = supplier().get();
        }
        dummy.serializeValue(value, buffer);
    }

    /**
     * Return a {@link ResultDataset} {@link Supplier}.
     * 
     * @return the supplier
     */
    protected abstract Supplier<ResultDataset<T>> supplier();

    /**
     * Populate {@link #tracking} if necessary and return it.
     * 
     * @return the {@link #tracking} copy of the data
     */
    protected final ResultDataset<T> tracking() {
        if(tracking == null) {
            tracking = supplier().get();
            data.forEach((entity, data) -> {
                data.forEach((attribute, values) -> {
                    values.forEach(
                            value -> tracking.insert(entity, attribute, value));
                });
            });
            data = tracking;
        }
        return tracking;
    }

}
