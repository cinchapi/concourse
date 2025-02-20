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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.ForwardingMap;

/**
 * A {@link Map} that transforms input data AND prettifies its
 * {@link #toString() string representation} just-in-time.
 *
 * @author Jeff Nelson
 */
abstract class PrettyTransformMap<KF, KT, VF, VT>
        extends ForwardingMap<KT, VT> {

    /**
     * The data whose values must be transformed.
     */
    private final Map<KF, VF> data;

    /**
     * A cache of the transformed results.
     */
    private Map<KT, VT> transformed = null;

    /**
     * A cache of the pretty results.
     */
    private Map<KT, VT> pretty = null;

    /**
     * Construct a new instance.
     * 
     * @param data
     */
    PrettyTransformMap(Map<KF, VF> data) {
        this.data = data;
    }

    @Override
    public boolean isEmpty() {
        // NOTE: This assumes that #transformKey won't produce any collisions
        return transformed == null ? data.isEmpty() : transformed.isEmpty();
    }

    @Override
    public int size() {
        // NOTE: This assumes that #transformKey won't produce any collisions
        return transformed == null ? data.size() : transformed.size();
    }

    @Override
    public final String toString() {
        if(pretty == null) {
            if(transformed != null) {
                // The data was previously transformed, so copy the results to a
                // new pretty map
                pretty = $prettyMapSupplier().get();
                entrySet().forEach(
                        entry -> pretty.put(entry.getKey(), entry.getValue()));
            }
            else {
                // The data was never transformed, so do the transformations
                // directly when generating the pretty results
                pretty = $transform($prettyMapSupplier());
            }
            transformed = pretty;
        }
        return pretty.toString();
    }

    /**
     * Return a {@link Supplier} of a {@link Map} instance that outputs a pretty
     * {@link #toString() string}.
     * 
     * @see {@link com.cinchapi.concourse.util.PrettyLinkedHashMap}
     * @see {@link com.cinchapi.concourse.util.PrettyLinkedTableMap}
     * @return an empty {@link Map}
     */
    protected abstract Supplier<Map<KT, VT>> $prettyMapSupplier();

    @Override
    protected Map<KT, VT> delegate() {
        if(transformed == null) {
            transformed = $transform(() -> new LinkedHashMap<>(data.size()));
        }
        return transformed;
    }

    /**
     * Transform the {@code value} to the appropriate type.
     * 
     * @param value
     * @return the transformed value.
     */
    protected abstract VT transformValue(VF value);

    /**
     * Transform the {@code key} to the appropriate type.
     * 
     * @param key
     * @return the transformed key
     */
    protected abstract KT transformKey(KF key);

    /**
     * Transform the {@link Map#values() values} in {@code data} within a
     * {@Map} that is provided by the {@code supplier}.
     * 
     * @param supplier
     * @return the transformed map
     */
    private final Map<KT, VT> $transform(Supplier<Map<KT, VT>> supplier) {
        // NOTE: This method does not use a Stream and Collector because that
        // approach relies on Map#merge, which does not hook into prettification
        // logic in PrettyLinkedHahMap and PrettyLinkedTableMap
        Map<KT, VT> transformed = supplier.get();
        for (Entry<KF, VF> entry : data.entrySet()) {
            transformed.put(transformKey(entry.getKey()),
                    transformValue(entry.getValue()));
        }
        return transformed;
    }

}
