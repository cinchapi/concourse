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
 * The {@link OrderState} that represents a transition induced by the
 * introduction of a timestamp token. The expected next token is either a
 * direction token or a new key to sort on.
 *
 * @author Jeff Nelson
 */
public class OrderAtState extends BuildableOrderState
        implements ShortcutThenByState, TransitionToOrderDirectionState {

    /**
     * Construct a new instance.
     * 
     * @param order
     */
    protected OrderAtState(BuiltOrder order, String key, Timestamp timestamp) {
        super(order);
        order.set(key, timestamp);
    }

    @Override
    public OrderThenState then() {
        return new OrderThenState(order);
    }

    @Override
    public BuiltOrder $order() {
        return order;
    }

}
