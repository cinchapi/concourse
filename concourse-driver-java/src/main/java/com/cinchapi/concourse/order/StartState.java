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
package com.cinchapi.concourse.order;

/**
 * The {@link StartState} marks the logical beginning of a new {@link Order}.
 */
public class StartState extends State {

    /**
     * Construct a new instance.
     *
     * @param Order
     */
    public StartState(Order Order) {
        super(Order);
    }

    /**
     * Add a {@code key} to the Order that is building.
     *
     * @param key
     * @return the builder
     */
    public ByState by(String key) {
        order.add(new KeySymbol(key));
        return new ByState(order);
    }

}
