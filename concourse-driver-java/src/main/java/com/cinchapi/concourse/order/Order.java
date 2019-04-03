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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.cinchapi.ccl.grammar.ParenthesisSymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A {@link Order} is an object that is used to encapsulate the semantics of
 * a sort order. Any given time, objects of this class can exist in one
 * of two modes: {@code building} or {@code built}. When a Order is
 * {@code built}, it is guaranteed to represent a fully and well formed sort order
 * that can be processed. On the other hand, when a Order is {@code building}
 * it is in an incomplete state.
 * <p>
 * This class is the public interface to Order construction. It is meant to
 * be used in a chained manner, where the caller initially calls
 * {@link Order#where()} and continues to construct the Order using the
 * options available from each subsequently returned state.
 * </p>
 *
 */
public class Order implements OrderSymbol {

    /**
     * Start building a new {@link Order}.
     *
     * @return the Order builder
     */
    public static StartState where() {
        return new StartState(new Order());
    }

    /**
     * A flag that indicates whether this {@link Order} has been built.
     */
    private boolean built = false;

    /**
     * The collection of {@link OrderSymbol}s that make up this {@link Order}.
     */
    private List<OrderSymbol> orderSymbols;

    /**
     * Construct a new instance.
     */
    protected Order() {
        this.orderSymbols = Lists.newArrayList();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Order) {
            return Objects.equals(orderSymbols, ((Order) obj).orderSymbols);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderSymbols);
    }

    /**
     * Add a {@link OrderSymbol} to this {@link Order}.
     *
     * @param orderSymbol
     */
    protected void add(OrderSymbol orderSymbol) {
        Preconditions.checkState(!built,
                "Cannot add a order symbol to a built Order");
        orderSymbols.add(orderSymbol);
    }

    /**
     * Mark this {@link Order} as {@code built}.
     */
    protected void close() {
        built = !built ? true : built;
        if (orderSymbols.get(orderSymbols.size()-1) instanceof KeySymbol) {
            orderSymbols.add(new AscendingSymbol());
        }
    }

    /**
     * Return the order list of {@link OrderSymbol} that make up this {@link Order}.
     *
     * @return orderSymbols
     */
    protected List<OrderSymbol> getOrderSymbols() {
        return Collections.unmodifiableList(orderSymbols);
    }

}
