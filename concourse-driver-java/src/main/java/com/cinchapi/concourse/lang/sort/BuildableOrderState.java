/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.util.List;

/**
 * The base class for a Sort Order state that can be transformed into a complete
 * and well-formed {@link BuiltOrder}.
 */
public abstract class BuildableOrderState extends OrderState implements Order {

    /**
     * Construct a new instance.
     *
     * @param order
     */
    protected BuildableOrderState(BuiltOrder order) {
        super(order);
    }

    /**
     * Build and return the {@link BuiltOrder}.
     *
     * @return the built Order
     */
    public final BuiltOrder build() {
        order.close();
        return order;
    }

    @Override
    public final List<OrderComponent> spec() {
        return build().spec();
    }

}
