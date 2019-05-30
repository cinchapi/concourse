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
package com.cinchapi.concourse.lang.sort;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cinchapi.concourse.Timestamp;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Implementation of {@link Order} that represents the result of calling
 * {@link BuildableOrderState#build()}.
 *
 * @author Jeff Nelson
 */
final class BuiltOrder implements Order {

    /**
     * A mapping from each key to direction ordinal (e.g. 1 for ASC and -1 for
     * DESC) in the constructed {@link BuiltOrder}.
     */
    private final LinkedHashMap<String, OrderComponent> spec;

    /**
     * The last key that was {@link #set(String, Direction) added}.
     */
    @Nullable
    protected String lastKey;

    /**
     * A flag that indicates whether this {@link BuiltOrder} has been built.
     */
    private boolean built = false;

    /**
     * Construct a new instance.
     */
    BuiltOrder() {
        this.spec = Maps.newLinkedHashMap();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof BuiltOrder) {
            return Objects.equals(spec, ((BuiltOrder) obj).spec);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return spec.hashCode();
    }

    @Override
    public List<OrderComponent> spec() {
        return spec.values().stream().collect(Collectors.toList());
    }

    /**
     * Modify to the order {@link #spec()}.
     * 
     * @param key
     * @param direction
     */
    final void set(String key, Direction direction) {
        OrderComponent component = spec.get(key);
        set(key, component == null ? null : component.timestamp(), direction);

    }

    /**
     * Mark this {@link BuiltOrder} as {@code built}.
     */
    void close() {
        built = !built ? true : built;
    }

    /**
     * Modify to the order {@link #spec()}.
     * 
     * @param key
     * @param timestamp
     * @param direction
     */
    final void set(String key, Timestamp timestamp) {
        OrderComponent component = spec.get(key);
        set(key, timestamp, component == null ? Direction.$default()
                : component.direction());
    }

    /**
     * Modify to the order {@link #spec()}.
     * 
     * @param key
     * @param timestamp
     * @param direction
     */
    final void set(String key, Timestamp timestamp, Direction direction) {
        Preconditions.checkState(!built, "Cannot modify a built Order");
        spec.put(key, new OrderComponent(key, timestamp, direction));
        this.lastKey = key;

    }

}
