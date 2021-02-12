/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
package com.cinchapi.concourse.server.query.paginate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.data.sort.SortableTable;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.server.query.sort.Sorting;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for the functionality contracted by the {@link Pageable} interface
 * as implemented in concourse result set wrappers.
 *
 * @author Jeff Nelson
 */
public class PaginationTest {

    @Test
    public void testPaginateSortedMap() {
        SortableTable<Set<TObject>> table = SortableTable
                .multiValued(Maps.newLinkedHashMap());
        table.put(1L, ImmutableMap.of("name",
                ImmutableSet.of(Convert.javaToThrift("a"))));
        table.put(2L, ImmutableMap.of("name",
                ImmutableSet.of(Convert.javaToThrift("b"))));
        table.put(3L, ImmutableMap.of("name",
                ImmutableSet.of(Convert.javaToThrift("c"))));
        table.put(4L, ImmutableMap.of("name",
                ImmutableSet.of(Convert.javaToThrift("d"))));
        table.put(5L, ImmutableMap.of("name",
                ImmutableSet.of(Convert.javaToThrift("e"))));
        table.put(6L, ImmutableMap.of("name",
                ImmutableSet.of(Convert.javaToThrift("z"))));
        table.sort(Sorting.byValues(Order.by("name").largestFirst(), null));
        Page page = Page.sized(2).go(2);
        Map<Long, Map<String, Set<TObject>>> expected = table.entrySet()
                .stream().skip(page.skip()).limit(page.limit())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                        (a, b) -> b, LinkedHashMap::new));
        Map<Long, Map<String, Set<TObject>>> actual = Paging.paginate(table,
                page);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testRandomAccessPaging() {
        Random random = new Random();
        int size = TestData.getScaleCount();
        List<Integer> list = Lists.newArrayList();
        for (int i = 0; i < size; ++i) {
            list.add(i);
        }
        int skip = Math.abs(random.nextInt(size - 1));
        int limit = Math.abs(random.nextInt(size - 1));
        Assert.assertEquals(
                list.stream().skip(skip).limit(limit)
                        .collect(Collectors.toList()),
                Lists.newArrayList(
                        Paging.paginate(list, Page.of(skip, limit))));
    }

    @Test
    public void testSequentialPaging() {
        Random random = new Random();
        int size = TestData.getScaleCount();
        Set<Integer> list = Sets.newHashSet();
        for (int i = 0; i < size; ++i) {
            list.add(i);
        }
        int skip = Math.abs(random.nextInt(size - 1));
        int limit = Math.abs(random.nextInt(size - 1));
        System.out.println(size);
        System.out.println(skip);
        System.out.println(limit);
        Assert.assertEquals(
                list.stream().skip(skip).limit(limit)
                        .collect(Collectors.toList()),
                Lists.newArrayList(
                        Paging.paginate(list, Page.of(skip, limit))));
    }

    @Test
    public void testSequentialPagingEdgeCase() {
        int size = 19;
        Set<Integer> list = Sets.newHashSet();
        for (int i = 0; i < size; ++i) {
            list.add(i);
        }
        int skip = 0;
        int limit = 0;
        Assert.assertEquals(
                list.stream().skip(skip).limit(limit)
                        .collect(Collectors.toList()),
                Lists.newArrayList(
                        Paging.paginate(list, Page.of(skip, limit))));
    }
}
