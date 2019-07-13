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

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
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

        // @formatter:off
        /*
         * ^^^ data set
        +====+=======+=======+======+==========+
        | id |  foo  |  bar  | name |   baz    |
        +====+=======+=======+======+==========+
        |  1 | @2    |       | A    | @3 @4    |
        +----+-------+-------+------+----------+
        |  2 | @3    | @1 @4 | B    |          |
        +----+-------+-------+------+----------+
        |  3 |       | @2    | C    | @1 @2 @5 |
        +----+-------+-------+------+----------+
        |  4 | @1 @2 | @3    | D    | @5 @3    |
        +----+-------+-------+------+----------+
        |  5 |       |       | E    |          |
        +----+-------+-------+------+----------+
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
        for (int i = 1; i <= 5; ++i) {
            client.add("count", i, i);
        }
        Number actual = client.calculate().sum("foo.bar.count");
        Assert.assertEquals(12, actual);
    }

}
