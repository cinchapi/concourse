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
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * {@link Order} encapsulates the semantics of a result set sorting. Any given
 * time, objects of this class can exist in one of two modes: {@code building}
 * or {@code built}. When an Order is
 * {@code built}, it is guaranteed to represent a fully and well formed sort
 * order that can be processed. On the other hand, when a Order is
 * {@code building} it is in an incomplete state.
 * <p>
 * This class is the public interface to Order construction. It is meant to
 * be used in a chained manner, where the caller initially calls
 * {@link Order#by} and continues to construct the Order using the
 * options available from each subsequently returned state.
 * </p>
 *
 */
public final class Order {

    /**
     * Start building a new {@link Order}.
     *
     * @return the Order builder
     */
    public static OrderByState by(String key) {
        Order order = new Order();
        return new OrderByState(order, key, Direction.$default());
    }

    /**
     * A mapping from each key to direction ordinal (e.g. 1 for ASC and -1 for
     * DESC) in the constructed {@link Order}.
     */
    @VisibleForTesting
    final LinkedHashMap<String, Integer> spec;

    /**
     * The last key that was {@link #add(String, Direction) added}.
     */
    @Nullable
    protected String lastKey;

    /**
     * A flag that indicates whether this {@link Order} has been built.
     */
    private boolean built = false;

    /**
     * Construct a new instance.
     */
    protected Order() {
        this.spec = Maps.newLinkedHashMap();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Order) {
            return Objects.equals(spec, ((Order) obj).spec);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return spec.hashCode();
    }

    /**
     * Mark this {@link Order} as {@code built}.
     */
    void close() {
        built = !built ? true : built;
    }

    public LinkedHashMap<String, Integer> getSpec() {
        return spec;
    }

    /**
     * Returns the internal order as a key/direction map
     *
     * @return the order mapping
     */
    protected LinkedHashMap<String, Integer> getSpec() {
        return spec;
    }

    /**
     * Add to the order {@link #spec}.
     * 
     * @param key
     * @param direction
     */
    protected final void add(String key, Direction direction) {
        Preconditions.checkState(!built, "Cannot modify a built Order");
        spec.put(key, direction.coefficient());
        this.lastKey = key;
    }

}
