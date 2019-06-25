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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link Order ordering} results.
 *
 * @author Jeff Nelson
 */
public class ResultOrderTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        seed();
    }

    @Test
    public void testSelectCclNoOrder() {
        Map<Long, Map<String, Set<Object>>> data = client
                .select("active = true");
        long last = 0;
        for (long record : data.keySet()) {
            Assert.assertTrue(record > last);
            last = record;
        }
    }

    @Test
    public void testSelectCclTimeNoOrder() {
        Timestamp timestamp = Timestamp.now();
        long id = client.find("active = true").iterator().next();
        client.set("name", "FooFooFoo", id);
        Map<Long, Map<String, Set<Object>>> data = client
                .select("active = true", timestamp);
        long last = 0;
        for (long record : data.keySet()) {
            Assert.assertTrue(record > last);
            last = record;
        }
        Assert.assertNotEquals("FooFooFoo",
                data.get(id).get("name").iterator().next());
    }

    @Test
    public void testGetCclNoOrder() {
        Map<Long, Map<String, Set<Object>>> data = client.get("active = true");
        long last = 0;
        for (long record : data.keySet()) {
            Assert.assertTrue(record > last);
            last = record;
        }
    }

    @Test
    public void testSelectCclOrder() {
        Map<Long, Map<String, Set<Object>>> data = client.select(
                "active = true", Order.by("score").then("name").largestFirst());
        String name = "";
        int score = 0;
        for (Map<String, Set<Object>> row : data.values()) {
            String $name = (String) row.get("name").iterator().next();
            int $score = (int) row.get("score").iterator().next();
            Assert.assertTrue(Integer.compare($score, score) > 0
                    || (Integer.compare($score, score) == 0
                            && $name.compareTo(name) < 0));
            name = $name;
            score = $score;
        }
    }

    @Test
    public void testSelectCclTimeOrder() {
        Timestamp timestamp = Timestamp.now();
        long id = client.find("active = true").iterator().next();
        client.set("name", "FooFooFoo", id);
        Map<Long, Map<String, Set<Object>>> data = client.select(
                "active = true", timestamp,
                Order.by("score").then("name").largestFirst());
        String name = "";
        int score = 0;
        for (Map<String, Set<Object>> row : data.values()) {
            String $name = (String) row.get("name").iterator().next();
            int $score = (int) row.get("score").iterator().next();
            Assert.assertTrue(Integer.compare($score, score) > 0
                    || (Integer.compare($score, score) == 0
                            && $name.compareTo(name) < 0));
            name = $name;
            score = $score;
        }
        Assert.assertNotEquals("FooFooFoo",
                data.get(id).get("name").iterator().next());
    }

    @Test
    public void testSelectCclTimeOrderWithTime() {
        long a = client.insert(ImmutableMap.of("name", "a", "age", 30));
        long b = client.insert(ImmutableMap.of("name", "b", "age", 30));
        Timestamp timestamp = Timestamp.now();
        client.set("name", "z", a);
        Map<Long, Map<String, Set<Object>>> data = client.select("age = 30",
                Order.by("name").at(timestamp));
        Assert.assertEquals(a, (long) data.keySet().iterator().next());
        data = client.select("age = 30", timestamp,
                Order.by("name").at(timestamp));
        Assert.assertEquals(a, (long) data.keySet().iterator().next());
        data = client.select("age = 30", timestamp,
                Order.by("name").at(Timestamp.now()));
        Assert.assertEquals(b, (long) data.keySet().iterator().next());
        data = client.select("age = 30", timestamp, Order.by("name"));
        Assert.assertEquals(a, (long) data.keySet().iterator().next());
    }

    @Test
    public void testGetCclOrder() {
        Map<Long, Map<String, Object>> data = client.get("active = true",
                Order.by("score").then("name").largestFirst());
        String name = "";
        int score = 0;
        for (Map<String, Object> row : data.values()) {
            String $name = (String) row.get("name");
            int $score = (int) row.get("score");
            Assert.assertTrue(Integer.compare($score, score) > 0
                    || (Integer.compare($score, score) == 0
                            && $name.compareTo(name) < 0));
            name = $name;
            score = $score;
        }
    }

    @Test
    public void testSelectKeyCclTimeOrderWithTime() {
        long a = client.insert(ImmutableMap.of("name", "a", "zage", 30));
        long b = client.insert(ImmutableMap.of("name", "b", "zage", 30));
        Timestamp timestamp = Timestamp.now();
        client.set("name", "z", a);
        Map<Long, Set<Object>> data = client.select("zage", "zage > 0",
                timestamp, Order.by("name"));
        Assert.assertEquals(a, (long) data.keySet().iterator().next());
        data = client.select("zage", "zage > 0", timestamp,
                Order.by("name").at(Timestamp.now()));
        Assert.assertEquals(b, (long) data.keySet().iterator().next());
    }

    @Test
    public void testFindCclNoOrder() {
        Set<Long> records = client.find("active = true");
        Assert.assertEquals(4, records.size());
    }

    @Test
    public void testFindCclOrder() {
        Set<Long> actual = client.find("active = true", Order.by("name"));
        Set<Long> expected = client.select("active = true", Order.by("name"))
                .keySet();
        Assert.assertArrayEquals(actual.toArray(), expected.toArray());
    }

    @Test
    public void testFindCriteriaNoOrder() {
        Set<Long> records = client.find(Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true));
        Assert.assertEquals(4, records.size());
    }

    @Test
    public void testFindCriteriaOrder() {
        Set<Long> actual = client.find(Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true), Order.by("name"));
        Set<Long> expected = client.select("active = true", Order.by("name"))
                .keySet();
        Assert.assertArrayEquals(actual.toArray(), expected.toArray());
    }

    /**
     * Seed the test database.
     */
    private void seed() {
        long a = client.insert(ImmutableMap.of("name", "Jeff Nelson", "age", 31,
                "company", "Cinchapi", "active", true, "score", 100));
        long b = client.insert(ImmutableMap.of("name", "John Doe", "age", 40,
                "company", "Blavity", "active", false, "score", 95));
        long c = client.insert(ImmutableMap.of("name", "Jeff Johnson", "age",
                38, "company", "BET", "active", true, "score", 99));
        long d = client.insert(ImmutableMap.of("name", "Ashleah Nelson", "age",
                31, "company", "Cinchapi", "active", false, "score", 100));
        long e = client.insert(ImmutableMap.of("name", "Barack Obama", "age",
                56, "company", "United States", "active", false, "score", 80));
        long f = client.insert(ImmutableMap.of("name", "Ronald Reagan", "age",
                89, "company", "United States", "active", false, "score", 80));
        long g = client.insert(ImmutableMap.of("name", "Zane Miller", "age", 31,
                "company", "Cinchapi", "active", true, "score", 100));
        long h = client.insert(ImmutableMap.of("name", "Peter Griffin", "age",
                40, "company", "Family Guy", "active", true, "score", 23));
        client.link("friends", ImmutableList.of(b, c, f, h), a);
        client.link("friends", ImmutableList.of(a, d, g), b);
        client.link("friends", ImmutableList.of(e), c);
        client.link("friends", ImmutableList.of(a, b, e, g, h), d);
        client.link("friends", ImmutableList.of(a), e);
        client.link("friends", ImmutableList.of(a, b, e, g, h), f);
        client.link("friends", ImmutableList.of(c, d, h), g);
        client.link("friends", ImmutableList.of(a, b, f), h);
    }

}
