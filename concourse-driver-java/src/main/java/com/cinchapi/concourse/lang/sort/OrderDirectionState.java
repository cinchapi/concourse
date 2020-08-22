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

/**
 * A {@link State} that represents a building {@link BuiltOrder} that just had
 * direction information specified for the most recently added key.
 *
 * @author Jeff Nelson
 */
public class OrderDirectionState extends BuildableOrderState implements
        ShortcutThenByState {

    /**
     * Construct a new instance.
     * 
     * @param order
     */
    OrderDirectionState(BuiltOrder order, String key, Direction direction) {
        super(order);
        order.set(key, direction);
    }

    @Override
    public OrderThenState then() {
        return new OrderThenState(order);
    }

}
