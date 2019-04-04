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

import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.SortOrder;
import com.cinchapi.concourse.lang.sort.SortOrderType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link Order} building
 * functionality.
 */
public class OrderTest {

    @Test
    public void testDefaultSortOrder() {
        List<SortOrder> expected = Lists
                .newArrayList(new SortOrder("foo", SortOrderType.ASCENDING));
        Order order = Order.by("foo").build();
        Assert.assertEquals(expected, order.getSortOrders());
    }

    @Test
    public void testAscendingSortOrder() {
        List<SortOrder> expected = Lists
                .newArrayList(new SortOrder("foo", SortOrderType.ASCENDING));
        Order order = Order.by("foo").ascending().build();
        Assert.assertEquals(expected, order.getSortOrders());
    }

    @Test
    public void testDescendingSortOrder() {
        List<SortOrder> expected = Lists
                .newArrayList(new SortOrder("foo", SortOrderType.DESCENDING));
        Order order = Order.by("foo").descending().build();
        Assert.assertEquals(expected, order.getSortOrders());
    }

    @Test
    public void testMultipleSortKeysSortOrder() {
        List<SortOrder> expected = Lists.newArrayList(
                new SortOrder("foo", SortOrderType.ASCENDING),
                new SortOrder("bar", SortOrderType.ASCENDING));
        Order order = Order.by("foo").then("bar").ascending().build();
        Assert.assertEquals(expected, order.getSortOrders());
    }

    @Test
    public void testMultipleSortKeysWithImplicitSortOrders() {
        List<SortOrder> expected = Lists.newArrayList(
                new SortOrder("foo", SortOrderType.ASCENDING),
                new SortOrder("bar", SortOrderType.ASCENDING),
                new SortOrder("zoo", SortOrderType.ASCENDING));
        Order order = Order.by("foo").then("bar").then("zoo").build();
        Assert.assertEquals(expected, order.getSortOrders());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltOrder() {
        Order order = Order.by("foo").build();
        order.add(new SortOrder("Bar", SortOrderType.ASCENDING));
    }

}
