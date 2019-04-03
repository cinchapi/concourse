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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Unit tests for the {@link com.cinchapi.concourse.order.Order} building
 * functionality.
 */
public class OrderTest {

    @Test
    public void testDefaultSortOrder() {
        List<OrderSymbol> expected = Lists.newArrayList(new KeySymbol("foo"),
                new AscendingSymbol());
        Order order = Order.by("foo").build();
        Assert.assertEquals(expected, order.getOrderSymbols());
    }

    @Test
    public void testAscendingSortOrder() {
        List<OrderSymbol> expected = Lists.newArrayList(new KeySymbol("foo"),
                new AscendingSymbol());
        Order order = Order.by("foo").ascending().build();
        Assert.assertEquals(expected, order.getOrderSymbols());
    }

    @Test
    public void testDescendingSortOrder() {
        List<OrderSymbol> expected = Lists.newArrayList(new KeySymbol("foo"),
                new DescendingSymbol());
        Order order = Order.by("foo").descending().build();
        Assert.assertEquals(expected, order.getOrderSymbols());
    }

    @Test
    public void testMultipleSortKeysSortOrder() {
        List<OrderSymbol> expected = Lists.newArrayList(new KeySymbol("foo"),
                new AscendingSymbol(), new KeySymbol("bar"), new AscendingSymbol());
        Order order = Order.by("foo").then("bar").ascending().build();
        Assert.assertEquals(expected, order.getOrderSymbols());
    }

    @Test
    public void testMultipleSortKeysWithImplicitSortOrders() {
        List<OrderSymbol> expected = Lists.newArrayList(new KeySymbol("foo"),
                new AscendingSymbol(), new KeySymbol("bar"), new AscendingSymbol(),
                new KeySymbol("zoo"), new AscendingSymbol());
        Order order = Order.by("foo").then("bar").then("zoo").build();
        Assert.assertEquals(expected, order.getOrderSymbols());
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotAddSymbolToBuiltOrder() {
        Order order = Order.by("foo").build();
        order.add(new KeySymbol("baz"));
    }

}
