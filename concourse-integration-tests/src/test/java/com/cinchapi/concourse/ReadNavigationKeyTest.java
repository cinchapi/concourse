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
package com.cinchapi.concourse;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for reading navigation keys.
 *
 * @author Jeff Nelson
 */
public class ReadNavigationKeyTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        client.link("foo", 2, 1);
        client.add("name", "A", 1);
        client.link("baz", 3, 1);
        client.link("baz", 4, 1);
        client.link("foo", 3, 2);
        client.link("bar", 1, 2);
        client.link("bar", 4, 2);
        client.add("name", "B", 2);
        client.link("bar", 2, 3);
        client.add("name", "C", 3);
        client.link("baz", 1, 3);
        client.link("baz", 2, 3);
        client.link("baz", 5, 3);
        client.link("foo", 1, 4);
        client.link("foo", 2, 4);
        client.link("bar", 3, 4);
        client.add("name", "D", 4);
        client.link("baz", 5, 4);
        client.link("baz", 3, 4);
        client.add("name", "E", 5);
        for (int i = 1; i <= 5; ++i) {
            client.add("count", i, i);
        }
        for (int i = 1; i <= 5; ++i) {
            if(i != 3) {
                client.add("scores", i * 10, i);
                client.add("scores", i * 100, i);
            }
        }
        // @formatter:off
        /*
         * ^^^ data set
        +====+========+========+======+============+=======+=========+
        | id |  foo   |  bar   | name |    baz     | count | scores  |
        +====+========+========+======+============+=======+=========+
        |  1 | @2     |        | A    | @3, @4     |     1 | 10, 100 |
        +----+--------+--------+------+------------+-------+---------+
        |  2 | @3     | @1, @4 | B    |            |     2 | 20, 200 |
        +----+--------+--------+------+------------+-------+---------+
        |  3 |        | @2     | C    | @1, @2, @5 |     3 |         |
        +----+--------+--------+------+------------+-------+---------+
        |  4 | @1, @2 | @3     | D    | @5, @3     |     4 | 40, 400 |
        +----+--------+--------+------+------------+-------+---------+
        |  5 |        |        | E    |            |     5 | 50, 500 |
        +----+--------+--------+------+------------+-------+---------+
        */
        // @formatter:on
    }

    @Test
    public void testSelectKeyRecord() {
        Set<String> names = client.select("foo.bar.name", 1);
        Assert.assertEquals(ImmutableSet.of("A", "D"), names);
    }

    @Test
    public void testSelectKeyRecordDeadEnd() {
        Set<String> names = client.select("bar.name", 1);
        Assert.assertEquals(ImmutableSet.of(), names);
        names = client.select("foo.baz.name", 1);
        Assert.assertEquals(ImmutableSet.of(), names);
    }

    @Test
    public void testBrowseKey() {
        Map<Object, Set<Long>> data = client.browse("foo.bar");
        Assert.assertEquals(ImmutableMap.of(Link.to(1), ImmutableSet.of(1L, 4L),
                Link.to(4), ImmutableSet.of(1L, 4L), Link.to(2),
                ImmutableSet.of(2L)), data);
    }

    @Test
    public void testSumKey() {
        Number actual = client.calculate().sum("foo.bar.count");
        Assert.assertEquals(12, actual);
    }

    @Test
    public void testAverageKey() {
        Number actual = client.calculate().average("bar.baz.scores");
        List<Number> scores = ImmutableList.of(40, 400, 50, 500, 10, 100, 20,
                200, 50, 500);
        Number expected = scores.stream().mapToLong(Number::longValue).average()
                .getAsDouble();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSelectKeysRecord() {
        Map<String, Set<Object>> data = client
                .select(ImmutableList.of("foo.bar.name", "baz.count"), 1);
        Assert.assertEquals(ImmutableMap.of("foo.bar.name",
                ImmutableSet.of("A", "D"), "baz.count", ImmutableSet.of(3, 4)),
                data);
    }

    @Test
    public void testSelectKeysRecords() {
        Map<Long, Map<String, Set<Object>>> data = client.select(
                ImmutableList.of("foo.bar.name", "baz.count"),
                ImmutableList.of(1L, 2L, 3L, 4L));
        System.out.println(data);
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableMap.of("foo.bar.name",
                ImmutableSet.of("A", "D"), "baz.count", ImmutableSet.of(3, 4)),
                2L,
                ImmutableMap.of("foo.bar.name", ImmutableSet.of("B"),
                        "baz.count", ImmutableSet.of()),
                3L,
                ImmutableMap.of("foo.bar.name", ImmutableSet.of(), "baz.count",
                        ImmutableSet.of(1, 2, 5)),
                4L, ImmutableMap.of("foo.bar.name", ImmutableSet.of("A", "D"),
                        "baz.count", ImmutableSet.of(5, 3))),
                data);
    }

}
