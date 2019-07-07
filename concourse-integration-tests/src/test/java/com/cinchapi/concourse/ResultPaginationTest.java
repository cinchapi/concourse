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
package com.cinchapi.concourse;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Unit tests for result set pagination functionality
 *
 * @author Jeff Nelson
 */
public class ResultPaginationTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        for (long i = 0; i < 100; ++i) {
            client.add("bar", i, i);
        }
    }

    @Test
    public void testSelectRecordsPage() {
        List<Long> records = Lists.newArrayList();
        for (long i = 0; i < 100; ++i) {
            client.add("foo", i, i);
            records.add(i);
        }
        List<Long> $records = Lists.newArrayList(records);
        Page page = Page.sized(15);
        Map<Long, Map<String, Set<Object>>> data = null;
        while (data == null || !data.isEmpty()) {
            data = client.select($records, page);
            for (long record : data.keySet()) {
                Assert.assertTrue(record >= page.skip()
                        && record <= (page.limit() + page.skip()));
            }
            records.removeAll(data.keySet());
            page = page.next();
        }
        Assert.assertTrue(records.isEmpty());
    }

    @Test
    public void testGetCclPage() {
        List<Long> records = Lists.newArrayList();
        for (long i = 0; i < 100; ++i) {
            client.add("foo", i, i);
            records.add(i);
        }
        Page page = Page.sized(15);
        Map<Long, Map<String, Set<Object>>> data = null;
        while (data == null || !data.isEmpty()) {
            data = client.get("foo >= 50", page);
            for (long record : data.keySet()) {
                Assert.assertTrue(record >= page.skip() + 50
                        && record <= (page.limit() + page.skip() + 50));
            }
            records.removeAll(data.keySet());
            page = page.next();
        }
    }

    @Test
    public void testFindCclPage() {
        Set<Long> records = client.find("bar > 30", Page.sized(7).go(3));
        Assert.assertEquals(7, records.size());
        Assert.assertEquals((long) (31 + 7 * 2),
                Iterables.getFirst(records, 0));
        Assert.assertEquals((long) ((31 + 7 * 2) + 6),
                Iterables.getLast(records, 0));
    }

    @Test
    public void testFindCclOrderPage() {
        Set<Long> records = client.find("bar > 30",
                Order.by("bar").largestFirst(), Page.sized(7).go(3));
        Assert.assertEquals(7, records.size());
        Assert.assertEquals((long) (99 - (7 * 2)),
                Iterables.getFirst(records, 0));
        Assert.assertEquals((long) (99 - (7 * 2) - 6),
                Iterables.getLast(records, 0));
    }

    @Test
    public void testFindCriteriaPage() {
        int size = 11;
        int page = 5;
        int value = 27;
        Set<Long> records = client.find(Criteria.where().key("bar")
                .operator(Operator.GREATER_THAN).value(value),
                Page.sized(size).go(page));
        Assert.assertEquals(size, records.size());
        Assert.assertEquals((long) ((value + 1) + size * (page - 1)),
                Iterables.getFirst(records, 0));
        Assert.assertEquals((long) (((value + 1) + size * 4) + (size - 1)),
                Iterables.getLast(records, 0));
    }

    @Test
    public void testFindCriteriaOrderPage() {
        int size = 11;
        int page = 5;
        int value = 27;
        Set<Long> records = client.find(
                Criteria.where().key("bar").operator(Operator.GREATER_THAN)
                        .value(value),
                Order.by("bar").smallestFirst(), Page.sized(size).go(page));
        Assert.assertEquals(size, records.size());
        Assert.assertEquals((long) ((value + 1) + size * (page - 1)),
                Iterables.getFirst(records, 0));
        Assert.assertEquals(
                (long) (((value + 1) + size * (page - 1)) + (size - 1)),
                Iterables.getLast(records, 0));
    }

    @Test
    public void testFindKeyOperatorValuePage() {
        int size = 11;
        int page = 1;
        int value = 27;
        Set<Long> records = client.find("bar", Operator.GREATER_THAN, value,
                Page.sized(size));
        Assert.assertEquals(size, records.size());
        Assert.assertEquals((long) ((value + 1) + size * (page - 1)),
                Iterables.getFirst(records, 0));
        Assert.assertEquals(
                (long) (((value + 1) + size * (page - 1)) + (size - 1)),
                Iterables.getLast(records, 0));
    }

    @Test
    public void testFindKeyOperatorValueTimePage() {
        Timestamp timestamp = Timestamp.now();
        for (int i = 100; i < 200; ++i) {
            client.add("bar", i, i);
        }
        Set<Long> records = client.find("bar", Operator.GREATER_THAN, 99,
                timestamp, Page.sized(10).next());
        Assert.assertTrue(records.isEmpty());
    }

    @Test
    public void testGetCclOrderPage() {
        Map<Long, Map<String, Object>> data = client.get("bar >< 10 20",
                Order.by("bar"), Page.sized(4).go(3));
        Assert.assertEquals((long) 10 + (4 * 2),
                (long) data.keySet().iterator().next());
    }

}
