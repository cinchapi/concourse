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
 * The {@link State} that expects the next token to be a sort order or a new key
 * to sort by.
 */
public class StartState extends BuildableState {

    /**
     * Construct a new instance.
     *
     * @param Order
     */
    protected StartState(Order Order) {
        super(Order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState ascending() {
        order.add(new AscendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState increasing() {
        order.add(new AscendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState smallestFirst() {
        order.add(new AscendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState largestLast() {
        order.add(new AscendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState descending() {
        order.add(new DescendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState decreasing() {
        order.add(new DescendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState reversed() {
        order.add(new DescendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState largestFirst() {
        order.add(new DescendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState smallestLast() {
        order.add(new DescendingSymbol());
        return new SortOrderState(order);
    }

    /**
     * Adds a new {@link KeySymbol} to sort by to the {@link Order} that is
     * building. Adds an implicit Ascending sort order to the prior key
     *
     * @return the builder
     */
    public ThenState then(String key) {
        order.add(new AscendingSymbol());
        order.add(new KeySymbol(key));
        return new ThenState(order);
    }
}
