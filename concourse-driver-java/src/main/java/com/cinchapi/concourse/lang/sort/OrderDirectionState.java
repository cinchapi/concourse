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
 * A {@link State} that represents a building {@link Order} that just had
 * direction information specified for the most recently added key.
 *
 * @author Jeff Nelson
 */
public class OrderDirectionState extends BuildableOrderState {

    /**
     * Construct a new instance.
     * 
     * @param order
     */
    OrderDirectionState(Order order, String key, Direction direction) {
        super(order);
        order.add(key, direction);
    }

    /**
     * Specify a transition to adding a new key to the order.
     * @return the builder
     */
    public OrderThenState then() {
        return new OrderThenState(order);
    }

}
