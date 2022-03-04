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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.cinchapi.ccl.grammar.DirectionSymbol;
import com.cinchapi.ccl.grammar.OrderSymbol;
import com.cinchapi.ccl.grammar.TimestampSymbol;
import com.cinchapi.ccl.syntax.OrderTree;
import com.cinchapi.concourse.Timestamp;

/**
 * {@link Order} encapsulates the semantics of a result set sorting. Any given
 * time, objects of this class can exist in one of two modes: {@code building}
 * or {@code built}. When an Order is {@code built}, it is guaranteed to
 * represent a fully and well formed sort order that can be processed. On the
 * other hand, when a Order is {@code building} it is in an incomplete state.
 * <p>
 * This class is the public interface to Order construction. It is meant to
 * be used in a chained manner, where the caller initially calls
 * {@link Order#by} and continues to construct the Order using the
 * options available from each subsequently returned state.
 * </p>
 */
public interface Order {

    /**
     * Start building a new {@link Order}.
     *
     * @return the Order builder
     */
    public static OrderByState by(String key) {
        BuiltOrder order = new BuiltOrder();
        return new OrderByState(order, key, Direction.$default());
    }

    /**
     * Return an {@link Order} based on the parsed statement that produced the
     * {@link OrderTree}.
     * 
     * @param tree
     * @return the associated {@link Order}
     */
    public static Order from(OrderTree tree) {
        return new Order() {

            @Override
            public List<OrderComponent> spec() {
                // TODO: this does not work if the order key is a function
                return ((OrderSymbol) tree.root()).components().stream()
                        .map(symbol -> new OrderComponent(
                                (String) symbol.key().key(),
                                symbol.timestamp() != TimestampSymbol.PRESENT
                                        ? Timestamp.fromMicros(
                                                symbol.timestamp().timestamp())
                                        : null,
                                symbol.direction() == DirectionSymbol.ASCENDING
                                        ? Direction.ASCENDING
                                        : Direction.DESCENDING))
                        .collect(Collectors.toList());
            }

        };
    }

    /**
     * Return an {@link Order} that specifies no order.
     * 
     * @return a no-op {@link Order}
     */
    public static Order none() {
        return NoOrder.INSTANCE;
    }

    /**
     * Return all the keys that are referenced by this {@link Order}.
     * 
     * @return a {@link Set} of all the keys that are referenced by this
     *         {@link Order}
     */
    public default Set<String> keys() {
        return spec().stream().map(OrderComponent::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Return the order specification, expressed as an ordered list of
     * {@link OrderComponent components} containing each key, an optional
     * {@link Timestamp} and the the corresponding direction ordinal (e.g. 1 for
     * ASC and -1 for DESC) in the constructed {@link Order}.
     * 
     * @return a sequential list of all the {@link OrderComponent}s that form
     *         this {@link Order}.
     */
    public List<OrderComponent> spec();

}
