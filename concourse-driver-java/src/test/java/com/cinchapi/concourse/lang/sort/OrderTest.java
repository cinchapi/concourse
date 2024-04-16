/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        ((BuiltOrder) order).append("bar");
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
        Order order = Order.by("name").at(t1).descending().then("age").then()
                .by("email").at(t2);
        List<OrderComponent> expected = ImmutableList.of(
                new OrderComponent("name", t1, Direction.DESCENDING),
                new OrderComponent("age", Direction.ASCENDING),
                new OrderComponent("email", t2, Direction.ASCENDING));
        Assert.assertEquals(expected, order.spec());
    }

    @Test
    public void testEmptyOrderWithTimestampsCorrectness() {
        Order order = Sort.by("name").then("age").then("foo").descending()
                .then("bar");
        Assert.assertTrue(order.keysWithTimestamps().isEmpty());
    }

    @Test
    public void testOrderWithTimestampsCorrectness() {
        Timestamp t1 = Timestamp.now();
        Timestamp t2 = Timestamp.now();
        Order order = Order.by("name").then("name").at(t1).then().by("age")
                .then().by("score").at(t2).then("foo").then("bar").at(t1)
                .then("name").at(t2);
        Map<String, Collection<Timestamp>> expected = new LinkedHashMap<>();
        expected.put("name", ImmutableList.of(t1, t2));
        expected.put("score", ImmutableList.of(t2));
        expected.put("bar", ImmutableList.of(t1));
        Assert.assertEquals(expected, order.keysWithTimestamps());
    }

    @Test
    public void testDeDuplicateOrderComponent() {
        Timestamp t1 = Timestamp.now();
        Order order = Order.by("name").then().by("name").at(t1).then()
                .by("name").descending().then("name").at(t1).then().by("name")
                .at(t1).decreasing();
        Assert.assertEquals(4, order.spec().size());
        Set<OrderComponent> unique = new HashSet<>(order.spec());
        Assert.assertEquals(unique.size(), order.spec().size());
    }

}
