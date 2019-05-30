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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for the {@link com.cinchapi.concourse.lang.sort.Order} building
 * functionality.
 */
public class OrderTest {

    @Test
    public void testDefaultSortOrder() {
        List<OrderComponent> expected = ImmutableList
                .of(new OrderComponent("foo", Direction.ASCENDING));
        Order order = Order.by("foo").build();
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testAscendingSortOrder() {
        List<OrderComponent> expected = ImmutableList
                .of(new OrderComponent("foo", Direction.ASCENDING));
        Order order = Order.by("foo").ascending().build();
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testDescendingSortOrder() {
        List<OrderComponent> expected = ImmutableList
                .of(new OrderComponent("foo", Direction.DESCENDING));
        Order order = Order.by("foo").descending().build();
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testMultipleSortKeysSortOrder() {
        List<OrderComponent> expected = ImmutableList.of(
                new OrderComponent("foo", Direction.ASCENDING),
                new OrderComponent("bar", Direction.ASCENDING));
        Order order = Order.by("foo").then().by("bar").ascending().build();
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testMultipleSortKeysWithImplicitSortOrders() {
        List<OrderComponent> expected = ImmutableList.of(
                new OrderComponent("foo", Direction.ASCENDING),
                new OrderComponent("bar", Direction.ASCENDING),
                new OrderComponent("zoo", Direction.ASCENDING));
        Order order = Order.by("foo").then().by("bar").then().by("zoo").build();
        Assert.assertEquals(expected, order.spec());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToOrder() {
        Order order = Order.by("foo").build();
        ((BuiltOrder) order).set("bar", Direction.ASCENDING);
    }

    @Test
    public void testAlias() {
        List<OrderComponent> expected = ImmutableList
                .of(new OrderComponent("foo", Direction.ASCENDING));
        Order order = Sort.by("foo").build();
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testComplexOrder() {
        Order order = Sort.by("a").then().by("b").ascending().then().by("c")
                .then().by("d").descending().then().by("e").largestFirst()
                .build();
        List<OrderComponent> expected = ImmutableList.of(
                new OrderComponent("a", Direction.ASCENDING),
                new OrderComponent("b", Direction.ASCENDING),
                new OrderComponent("c", Direction.ASCENDING),
                new OrderComponent("d", Direction.DESCENDING),
                new OrderComponent("e", Direction.DESCENDING));
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testComplexOrderWithShortcut() {
        Order order = Sort.by("a").then("b").ascending().then("c").then()
                .by("d").descending().then("e").largestFirst().build();
        List<OrderComponent> expected = ImmutableList.of(
                new OrderComponent("a", Direction.ASCENDING),
                new OrderComponent("b", Direction.ASCENDING),
                new OrderComponent("c", Direction.ASCENDING),
                new OrderComponent("d", Direction.DESCENDING),
                new OrderComponent("e", Direction.DESCENDING));
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testOrderWithTimestamp() {
        Timestamp t1 = Timestamp.now();
        Timestamp t2 = Timestamp.now();
        Order order = Order.by("name").at(t1).descending()
                .then("age").then().by("email")
                .at(t2);
        List<OrderComponent> expected = ImmutableList.of(
                new OrderComponent("name", t1, Direction.DESCENDING),
                new OrderComponent("age", Direction.ASCENDING),
                new OrderComponent("email", t2, Direction.ASCENDING));
        Assert.assertEquals(expected, order.spec());
    }

}
