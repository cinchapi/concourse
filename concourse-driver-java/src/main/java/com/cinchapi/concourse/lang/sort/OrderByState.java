/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import com.cinchapi.concourse.Timestamp;

/**
 * The {@link OrderState} that expects the next token to be a sort order,
 * timestamp or a new key to sort by.
 */
public class OrderByState extends BuildableOrderState implements
        ShortcutThenByState,
        TransitionToOrderDirectionState {

    /**
     * Construct a new instance.
     *
     * @param order
     */
    OrderByState(BuiltOrder order, String key, Direction direction) {
        super(order);
        order.set(key, direction);
    }

    @Override
    public OrderThenState then() {
        return new OrderThenState(order);
    }

    public OrderAtState at(Timestamp timestamp) {
        return new OrderAtState(order, order.lastKey, timestamp);
    }

    @Override
    public BuiltOrder $order() {
        return order;
    }
}
