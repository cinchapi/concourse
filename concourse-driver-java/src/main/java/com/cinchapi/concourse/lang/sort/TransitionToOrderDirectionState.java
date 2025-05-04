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
package com.cinchapi.concourse.lang.sort;

/**
 * A {@link OrderState} that may transition to an {@link OrderDirectionState}.
 *
 * @author Jeff Nelson
 */
interface TransitionToOrderDirectionState {

    /**
     * Add the {@link Direction#ASCENDING} direction to the last key that
     * was specified in {@link BuiltOrder} that is building
     *
     * @return the builder
     */
    public default OrderDirectionState ascending() {
        return new OrderDirectionState($order(), $order().lastKey,
                Direction.ASCENDING);
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState decreasing() {
        return descending();
    }

    /**
     * Add the {@link Direction#DESCENDING} direction to the last key that
     * was specified in {@link BuiltOrder} that is building
     *
     * @return the builder
     */
    public default OrderDirectionState descending() {
        return new OrderDirectionState($order(), $order().lastKey,
                Direction.DESCENDING);
    }

    /**
     * Alias for {@link #ascending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState increasing() {
        return ascending();
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState largestFirst() {
        return descending();
    }

    /**
     * Alias for {@link #ascending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState largestLast() {
        return ascending();
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState reversed() {
        return descending();
    }

    /**
     * Alias for {@link #ascending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState smallestFirst() {
        return ascending();
    }

    /**
     * Alias for {@link #descending()}.
     *
     * @return the builder
     */
    public default OrderDirectionState smallestLast() {
        return descending();
    }

    /**
     * Return the {@link BuiltOrder} that is being constructed.
     * 
     * @return the {@link BuiltOrder}
     */
    public BuiltOrder $order();

}
