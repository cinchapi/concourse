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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for the {@link com.cinchapi.concourse.lang.sort.Order} building
 * functionality.
 */
public class OrderTest {

    @Test
    public void testDefaultSortOrder() {
        Map<String, Integer> expected = ImmutableMap.of("foo", 1);
        Order order = Order.by("foo").build();
        Assert.assertEquals(expected, order.spec);
    }

    @Test
    public void testAscendingSortOrder() {
        Map<String, Integer> expected = ImmutableMap.of("foo", 1);
        Order order = Order.by("foo").ascending().build();
        Assert.assertEquals(expected, order.spec);
    }

    @Test
    public void testDescendingSortOrder() {
        Map<String, Integer> expected = ImmutableMap.of("foo", -1);
        Order order = Order.by("foo").descending().build();
        Assert.assertEquals(expected, order.spec);
    }

    @Test
    public void testMultipleSortKeysSortOrder() {
        Map<String, Integer> expected = ImmutableMap.of("foo", 1, "bar", 1);
        Order order = Order.by("foo").then().by("bar").ascending().build();
        Assert.assertEquals(expected, order.spec);
    }

    @Test
    public void testMultipleSortKeysWithImplicitSortOrders() {
        Map<String, Integer> expected = ImmutableMap.of("foo", 1, "bar", 1,
                "zoo", 1);
        Order order = Order.by("foo").then().by("bar").then().by("zoo").build();
        Assert.assertEquals(expected, order.spec);
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltOrder() {
        Order order = Order.by("foo").build();
        order.add("bar", Direction.ASCENDING);
    }

    @Test
    public void testAlias() {
        Map<String, Integer> expected = ImmutableMap.of("foo", 1);
        Order order = Sort.by("foo").build();
        Assert.assertEquals(expected, order.spec);
    }

    @Test
    public void testComplexOrder() {
        Order order = Sort.by("a").then().by("b").ascending().then().by("c")
                .then().by("d").descending().then().by("e").largestFirst()
                .build();
        Map<String, Integer> expected = ImmutableMap.of("a", 1, "b", 1, "c", 1,
                "d", -1, "e", -1);
        Assert.assertEquals(expected, order.spec);
    }

    @Test
    public void testComplexOrderWithShortcut() {
        Order order = Sort.by("a").then("b").ascending().then("c").then()
                .by("d").descending().then("e").largestFirst().build();
        Map<String, Integer> expected = ImmutableMap.of("a", 1, "b", 1, "c", 1,
                "d", -1, "e", -1);
        Assert.assertEquals(expected, order.spec);
    }

}
