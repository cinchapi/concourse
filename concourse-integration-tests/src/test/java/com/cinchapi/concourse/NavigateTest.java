/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tests to check the functionality of navigate feature.
 * 
 * @author Raghav Babu
 */
public class NavigateTest extends ConcourseIntegrationTest {

    /**
     * Setup data for each of the tests.
     * 
     * @param client
     */
    private static Map<Long, Set<String>> setupNavigateKeyCriteria(
            Concourse client) {
        client.add("name", "foo", 1);
        client.add("age", 30, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Set<String>> expected = Maps.newHashMap();
        expected.put(3L, Sets.newHashSet("hello"));
        return expected;
    }

    /**
     * Setup data for each of the tests.
     * 
     * @param client
     */
    private static Map<Long, Set<String>> setupNavigateKeyRecord(
            Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        Map<Long, Set<String>> expected = Maps.newHashMap();
        expected.put(2L, Sets.newHashSet("bar"));
        return expected;
    }

    /**
     * Setup data for each of the tests.
     * 
     * @param client
     */
    private static Map<Long, Set<String>> setupNavigateKeyRecords(
            Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Set<String>> expected = Maps.newHashMap();
        expected.put(2L, Sets.newHashSet("bar"));
        expected.put(3L, Sets.newHashSet("raghav"));
        expected.put(4L, Sets.newHashSet("jeff"));
        return expected;
    }

    /**
     * Setup data for each of the tests.
     * 
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateKeysCriteria(
            Concourse client) {
        client.add("name", "foo", 1);
        client.add("age", 35, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row2 = Maps.newHashMap();
        row2.put("name", Sets.newHashSet("bar"));
        Map<String, Set<Object>> row3 = Maps.newHashMap();
        row3.put("name", Sets.newHashSet("hello"));
        expected.put(2L, row2);
        expected.put(3L, row3);
        return expected;
    }

    /**
     * Setup data for each of the tests.
     * 
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateKeysRecord(
            Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row2 = Maps.newHashMap();
        Map<String, Set<Object>> row3 = Maps.newHashMap();
        Map<String, Set<Object>> row4 = Maps.newHashMap();
        expected.put(2L, row2);
        expected.put(3L, row3);
        expected.put(4L, row4);
        row2.put("name", Sets.newHashSet("bar"));
        row3.put("name", Sets.newHashSet("raghav"));
        row4.put("name", Sets.newHashSet("jeff"));
        return expected;
    }

    /**
     * Setup data for each of the tests.
     * 
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateKeysRecords(
            Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row2 = Maps.newHashMap();
        Map<String, Set<Object>> row3 = Maps.newHashMap();
        Map<String, Set<Object>> row4 = Maps.newHashMap();
        expected.put(2L, row2);
        expected.put(3L, row3);
        expected.put(4L, row4);
        row2.put("name", Sets.newHashSet("bar"));
        row3.put("name", Sets.newHashSet("raghav"));
        row4.put("name", Sets.newHashSet("jeff"));
        return expected;
    }

    @Test
    public void testNavigateKeyCcl() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyCriteria(client);
        expected.put(3L, Sets.newHashSet("hello"));
        Map<Long, Set<String>> actual = client.navigate(direction, "age < 30");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyCclTime() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyCriteria(client);
        expected.put(3L, Sets.newHashSet("hello"));
        Timestamp timestamp = Timestamp.now();
        client.set("name", "john", 3);
        Map<Long, Set<String>> actual = client.navigate(direction, "age < 30",
                timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyCriteria() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyCriteria(client);
        expected.put(3L, Sets.newHashSet("hello"));
        Map<Long, Set<String>> actual = client.navigate(direction,
                Criteria.where().key("age").operator(Operator.LESS_THAN)
                        .value(30).build());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyCriteriaTime() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyCriteria(client);
        expected.put(3L, Sets.newHashSet("hello"));
        Timestamp timestamp = Timestamp.now();
        client.set("name", "john", 3);
        Map<Long, Set<String>> actual = client.navigate(
                direction, Criteria.where().key("age")
                        .operator(Operator.LESS_THAN).value(30).build(),
                timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyRecord() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyRecord(client);
        Map<Long, Set<String>> actual = client.navigate(direction, 1);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyRecords() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyRecords(client);
        Map<Long, Set<String>> actual = client.navigate(direction,
                Lists.newArrayList(1L, 2L));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyRecordsTime() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyRecords(client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "john", 3);
        Map<Long, Set<String>> actual = client.navigate(direction,
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeyRecordTime() {
        String direction = "friends.name";
        Map<Long, Set<String>> expected = setupNavigateKeyRecord(client);
        Timestamp timestamp = Timestamp.now();
        client.add("name", "john", 3);
        Map<Long, Set<String>> actual = client.navigate(direction, 1,
                timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeysCcl() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateKeysCriteria(
                client);
        Map<Long, Map<String, Set<String>>> actual = client.navigate(
                Lists.newArrayList("friends.name", "friends.friends.name"),
                "age > 30");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeysCclTime() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateKeysCriteria(
                client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "foo", 2);
        Map<Long, Map<String, Set<String>>> actual = client.navigate(
                Lists.newArrayList("friends.name", "friends.friends.name"),
                "age > 30", timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeysRecord() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateKeysRecord(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client.navigate(
                Lists.newArrayList("friends.name", "friends.friends.name"), 1);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeysRecords() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateKeysRecords(
                client);
        Map<Long, Map<String, Set<String>>> actual = client.navigate(
                Lists.newArrayList("friends.name", "friends.friends.name"),
                Lists.newArrayList(1L, 2L));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeysRecordsTime() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateKeysRecords(
                client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "jefferson", 4);
        Map<Long, Map<String, Set<String>>> actual = client.navigate(
                Lists.newArrayList("friends.name", "friends.friends.name"),
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateKeysRecordTime() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateKeysRecord(
                client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "jeffery", 4);
        Map<Long, Map<String, Set<Object>>> actual = client.navigate(
                Lists.newArrayList("friends.name", "friends.friends.name"), 1,
                timestamp);
        Assert.assertEquals(expected, actual);
    }

}
