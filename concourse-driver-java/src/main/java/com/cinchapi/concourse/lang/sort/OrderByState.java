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

/**
 * The {@link OrderState} that expects the next token to be a sort order or a
 * new key
 * to sort by.
 */
public class OrderByState extends BuildableOrderState implements
        ShortcutThenByState {

    /**
     * Construct a new instance.
     *
     * @param order
     */
    OrderByState(Order order, String key, Direction direction) {
        super(order);
        order.add(key, direction);
    }

    /**
     * Add the {@link Direction#ASCENDING} direction to the last key that
     * was specified in {@link Order} that is building
     *
     * @return the builder
     */
    public OrderDirectionState ascending() {
        return new OrderDirectionState(order, order.lastKey,
                Direction.ASCENDING);
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public OrderDirectionState decreasing() {
        return descending();
    }

    /**
     * Add the {@link Direction#DESCENDING} direction to the last key that
     * was specified in {@link Order} that is building
     *
     * @return the builder
     */
    public OrderDirectionState descending() {
        return new OrderDirectionState(order, order.lastKey,
                Direction.DESCENDING);
    }

    /**
     * Alias for {@link #ascending()}.
     *
     * @return the builder
     */
    public OrderDirectionState increasing() {
        return ascending();
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public OrderDirectionState largestFirst() {
        return descending();
    }

    /**
     * Alias for {@link #ascending()}.
     *
     * @return the builder
     */
    public OrderDirectionState largestLast() {
        return ascending();
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public OrderDirectionState reversed() {
        return descending();
    }

    /**
     * Alias for {@link #ascending()}.
     *
     * @return the builder
     */
    public OrderDirectionState smallestFirst() {
        return ascending();
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public OrderDirectionState smallestLast() {
        return descending();
    }

    @Override
    public OrderThenState then() {
        return new OrderThenState(order);
    }
}
