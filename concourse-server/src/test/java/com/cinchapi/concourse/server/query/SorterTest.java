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
package com.cinchapi.concourse.server.query;

import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TMaps;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link Sorter}.
 */
public class SorterTest {

    @Test
    public void testIntAscendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("jeffB"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Blavity"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(25));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(4), entry);

        Order order = Order.by("age").ascending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(4));
        expectedSort.add(Integer.toUnsignedLong(1));
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(3));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }


    @Test
    public void testIntDescendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("jeffB"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Blavity"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(25));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(4), entry);

        Order order = Order.by("age").descending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(3));
        expectedSort.add(Integer.toUnsignedLong(1));
        expectedSort.add(Integer.toUnsignedLong(4));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }

    @Test
    public void testStringAscendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("jeffB"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Blavity"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3), entry);

        entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(25));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(4), entry);

        Order order = Order.by("name").ascending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(3));
        expectedSort.add(Integer.toUnsignedLong(1));
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(4));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }

    @Test
    public void testMissingKeyAscendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("jeffB"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Blavity"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(4),
                entry);

        Order order = Order.by("age").ascending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(1));
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(4));
        expectedSort.add(Integer.toUnsignedLong(3));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }

    @Test
    public void testMissingKeyDescendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("jeffB"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Blavity"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(4),
                entry);

        Order order = Order.by("age").descending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(4));
        expectedSort.add(Integer.toUnsignedLong(1));
        expectedSort.add(Integer.toUnsignedLong(3));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }

    @Test
    public void testUnequalValueQuantityAscendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"), Convert.javaToThrift("Blavity"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(25));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3),
                entry);

        Order order = Order.by("company").ascending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(1));
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(3));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }

    @Test
    public void testEqualValueQuantityAscendingSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100), Convert.javaToThrift(20));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100), Convert.javaToThrift(25));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3),
                entry);

        Order order = Order.by("age").ascending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(3));
        expectedSort.add(Integer.toUnsignedLong(1));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }

    @Test
    public void testSortMultipleKeysSort() {
        Map<Long, Map<String, Set<TObject>>> records = Maps.newLinkedHashMap();

        List<String> keys = Lists.newArrayList("name", "company", "age");

        Map<String, Set<TObject>> entry = TMaps
                .newLinkedHashMapWithCapacity(keys.size());
        Set<TObject> values = Sets.newHashSet(Convert.javaToThrift("jeff"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(1),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("ashleah"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("ARMN Inc."));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(100));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(2),
                entry);

        entry = TMaps.newLinkedHashMapWithCapacity(keys.size());
        values = Sets.newHashSet(Convert.javaToThrift("mark"));
        entry.put("name", values);
        values = Sets.newHashSet(Convert.javaToThrift("Cinchapi"));
        entry.put("company", values);
        values = Sets.newHashSet(Convert.javaToThrift(50));
        entry.put("age", values);
        TMaps.putResultDatasetOptimized(records, Integer.toUnsignedLong(3),
                entry);

        Order order = Order.by("age").ascending().then("company").ascending().build();

        Map<Long, Map<String, Set<TObject>>> result = Sorter.sort(records, order);

        List<Long> expectedSort = Lists.newArrayList();
        expectedSort.add(Integer.toUnsignedLong(3));
        expectedSort.add(Integer.toUnsignedLong(2));
        expectedSort.add(Integer.toUnsignedLong(1));

        List<Long> sort = Lists.newArrayList(result.keySet());

        Assert.assertEquals(expectedSort, sort);
    }
}
