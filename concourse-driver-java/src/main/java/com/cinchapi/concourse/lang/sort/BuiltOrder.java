/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cinchapi.concourse.Timestamp;
import com.google.common.base.Preconditions;

/**
 * Implementation of {@link Order} that represents the result of calling
 * {@link BuildableOrderState#build()}.
 *
 * @author Jeff Nelson
 */
final class BuiltOrder implements Order {

    /**
     * A list of all the {@link OrderComponent components} that have been added.
     */
    private List<OrderComponent> spec;

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
        this.spec = new ArrayList<>();
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
        return Collections.unmodifiableList(spec);
    }

    /**
     * Modify the last {@link OrderComponent} and set the {@code direction}.
     * 
     * @param direction
     */
    final void append(Direction direction) {
        Preconditions.checkState(spec.size() > 0);
        OrderComponent component = spec.remove(spec.size() - 1);
        push(component.key(), component.timestamp(), direction);
    }

    /**
     * Append a new {@link OrderComponent} for {@code key}.
     * 
     * @param key
     */
    final void append(String key) {
        push(key, null, Direction.$default());
    }

    /**
     * Modify the last {@link OrderComponent} and set the {@code timestamp}.
     * 
     * @param direction
     */
    final void append(Timestamp timestamp) {
        Preconditions.checkState(spec.size() > 0);
        OrderComponent component = spec.remove(spec.size() - 1);
        push(component.key(), timestamp, component.direction());
    }

    /**
     * Mark this {@link BuiltOrder} as {@code built}.
     */
    void close() {
        spec = spec.stream().distinct().collect(Collectors.toList());
        built = !built ? true : built;
    }

    /**
     * Create an {@link OrderComponent} encompassing {@code key},
     * {@code timestamp} and {@code direction} and add it to the {@link #spec}.
     * 
     * @param key
     * @param timestamp
     * @param direction
     */
    private final void push(String key, Timestamp timestamp,
            Direction direction) {
        Preconditions.checkState(!built, "Cannot modify a built Order");
        spec.add(new OrderComponent(key, timestamp, direction));
        this.lastKey = key;
    }

    /**
     * Convenience method for ManagedConcourseServer to populate a remote order.
     * 
     * @param key
     * @param timestamp
     * @param direction
     */
    @SuppressWarnings("unused")
    private void set(String key, long timestamp, int direction) {
        push(key, timestamp > 0 ? Timestamp.fromMicros(timestamp) : null,
                Direction.values()[direction]);
    }

}
