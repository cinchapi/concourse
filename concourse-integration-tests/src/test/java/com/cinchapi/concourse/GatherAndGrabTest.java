package com.cinchapi.concourse;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class GatherAndGrabTest extends ConcourseIntegrationTest {

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupNavigateKeyCriteria(Concourse client) {
        client.add("name", "foo", 1);
        client.add("age", 30, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupNavigateKeyRecord(Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupNavigateKeyRecords(Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupNavigateKeysCriteria(Concourse client) {
        client.add("name", "foo", 1);
        client.add("age", 35, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupNavigateKeysRecord(Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupNavigateKeysRecords(Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
    }

    @Test public void testGatherRecord() {
        setupNavigateKeysRecord(client);
        Map<String, Set<Object>> select = client.select(1);
        Map<String, Set<Object>> actual = client.gather(1);
        Assert.assertEquals(ImmutableMap.of("friends",
                ImmutableSet.of(Link.to(2)), "name", ImmutableSet.of("foo")),
                actual);
        Assert.assertEquals(select,
                actual);
    }

    @Test public void testNavigateKeyCclTime() {
        String direction = "friends.name";
        setupNavigateKeyCriteria(client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "john", 3);
        Map<Long, Set<String>> actual = client
                .select(direction, "age < 30", timestamp);
        Assert.assertEquals(ImmutableMap.of(2L, ImmutableSet.of("hello")),
                actual);
    }

    @Test public void testNavigateKeyCriteria() {
        String direction = "friends.name";
        setupNavigateKeyCriteria(client);
        Map<Long, Set<String>> actual = client.select(direction,
                Criteria.where().key("age").operator(Operator.LESS_THAN)
                        .value(30).build());
        Assert.assertEquals(ImmutableMap.of(2L, ImmutableSet.of("hello")),
                actual);
    }

    @Test public void testNavigateKeyCriteriaTime() {
        String direction = "friends.name";
        setupNavigateKeyCriteria(client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "john", 3);
        Map<Long, Set<String>> actual = client.select(direction,
                Criteria.where().key("age").operator(Operator.LESS_THAN).value(30).build(),
                timestamp);
        Assert.assertEquals(ImmutableMap.of(2L, ImmutableSet.of("hello")),
                actual);
    }

    @Test public void testNavigateKeyRecord() {
        String direction = "friends.name";
        setupNavigateKeyRecord(client);
        Set<String> actual = client.select(direction, 1);
        Assert.assertEquals(ImmutableSet.of("bar"), actual);
    }

    @Test public void testNavigateKeyRecords() {
        String direction = "friends.name";
        setupNavigateKeyRecords(client);
        Map<Long, Set<String>> actual = client
                .select(direction, Lists.newArrayList(1L, 2L));
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of("bar"), 2L,
                ImmutableSet.of("raghav", "jeff")), actual);
    }

    @Test public void testNavigateKeyRecordsTime() {
        String direction = "friends.name";
        setupNavigateKeyRecords(client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "john", 3);
        Map<Long, Set<String>> actual = client
                .select(direction, Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of("bar"), 2L,
                ImmutableSet.of("raghav", "jeff")), actual);
    }

    @Test public void testNavigateKeyRecordTime() {
        String direction = "friends.name";
        setupNavigateKeyRecord(client);
        Timestamp timestamp = Timestamp.now();
        client.add("name", "john", 3);
        Set<String> actual = client.select(direction, 1, timestamp);
        Assert.assertEquals(ImmutableSet.of("bar"), actual);
    }

    @Test public void testNavigateKeysCcl() {
        setupNavigateKeysCriteria(client);
        Map<Long, Map<String, Set<String>>> actual = client
                .get(Lists.newArrayList("friends.name", "friends.friends.name"),
                        "age > 30");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableMap
                        .of("friends.name", "bar", "friends.friends.name", "hello")),
                actual);
    }

    @Test public void testNavigateKeysCclTime() {
        setupNavigateKeysCriteria(client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "foo", 2);
        Map<Long, Map<String, Set<String>>> actual = client
                .get(Lists.newArrayList("friends.name", "friends.friends.name"),
                        "age > 30", timestamp);
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableMap
                        .of("friends.name", "bar", "friends.friends.name", "hello")),
                actual);
    }

    @Test public void testNavigateKeysRecord() {
        setupNavigateKeysRecord(client);
        Map<String, Set<Object>> actual = client
                .get(Lists.newArrayList("friends.name", "friends.friends.name"),
                        1);
        Assert.assertEquals(ImmutableMap
                        .of("friends.name", "bar", "friends.friends.name", "jeff"),
                actual);
    }

    @Test public void testNavigateKeysRecords() {
        setupNavigateKeysRecords(client);
        Map<Long, Map<String, Set<String>>> actual = client.select(Lists
                        .newArrayList("friends.name", "friends.friends.name"),
                Lists.newArrayList(1L, 2L));
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableMap
                        .of("friends.name", ImmutableSet.of("bar"),
                                "friends.friends.name", ImmutableSet.of("raghav", "jeff")),
                2L, ImmutableMap
                        .of("friends.name", ImmutableSet.of("raghav", "jeff"),
                                "friends.friends.name", ImmutableSet.of())),
                actual);
    }

    @Test public void testNavigateKeysRecordsTime() {
        setupNavigateKeysRecords(client);
        Timestamp timestamp = Timestamp.now();
        client.set("name", "jefferson", 4);
        Map<Long, Map<String, Set<String>>> actual = client.select(Lists
                        .newArrayList("friends.name", "friends.friends.name"),
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableMap
                        .of("friends.name", ImmutableSet.of("bar"),
                                "friends.friends.name", ImmutableSet.of("raghav", "jeff")),
                2L, ImmutableMap
                        .of("friends.name", ImmutableSet.of("raghav", "jeff"),
                                "friends.friends.name", ImmutableSet.of())),
                actual);
    }

    @Test public void testNavigateKeysRecordTime() {
        setupNavigateKeysRecord(client);
        Timestamp timestamp = Timestamp.now();
        client.add("name", "jeffery", 4);
        Map<String, Set<Object>> actual = client
                .get(Lists.newArrayList("friends.name", "friends.friends.name"),
                        1, timestamp);
        Assert.assertEquals(ImmutableMap
                        .of("friends.name", "bar", "friends.friends.name", "jeff"),
                actual);
    }
}
