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
 * The {@link OrderState} that expects the current token to be a sort order or a
 * sort key.
 */
public class ThenOrderState extends BuildableOrderState {

    /**
     * Construct a new instance.
     *
     * @param Order
     */
    protected ThenOrderState(Order Order) {
        super(Order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState ascending() {
        order.getSortOrders().get(order.getSortOrders().size() - 1).ascending();
        return new SortOrderState(order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState increasing() {
        order.getSortOrders().get(order.getSortOrders().size() - 1).ascending();
        return new SortOrderState(order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState smallestFirst() {
        order.getSortOrders().get(order.getSortOrders().size() - 1).ascending();
        return new SortOrderState(order);
    }

    /**
     * Add an ascending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState largestLast() {
        order.getSortOrders().get(order.getSortOrders().size() - 1).ascending();
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState descending() {
        order.getSortOrders().get(order.getSortOrders().size() - 1)
                .descending();
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState decreasing() {
        order.getSortOrders().get(order.getSortOrders().size() - 1)
                .descending();
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState reversed() {
        order.getSortOrders().get(order.getSortOrders().size() - 1)
                .descending();
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState largestFirst() {
        order.getSortOrders().get(order.getSortOrders().size() - 1)
                .descending();
        return new SortOrderState(order);
    }

    /**
     * Add a descending sort order to the {@link Order} that is building
     *
     * @return the builder
     */
    public SortOrderState smallestLast() {
        order.getSortOrders().get(order.getSortOrders().size() - 1)
                .descending();
        return new SortOrderState(order);
    }

    /**
     * Adds a new {@link SortOrder} to sort by to the {@link Order} that is
     * building. Adds an implicit Ascending sort order to the prior key
     *
     * @return the builder
     */
    public ThenOrderState then(String key) {
        order.add(new SortOrder(key));
        return new ThenOrderState(order);
    }

}