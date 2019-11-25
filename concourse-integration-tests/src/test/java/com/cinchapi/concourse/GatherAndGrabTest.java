package com.cinchapi.concourse;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GatherAndGrabTest extends ConcourseIntegrationTest {

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
    private static void setupNavigateKeysRecord(Concourse client) {
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("family", Link.to(3), 1);
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

    @Test
    public void testGrabKeyRecord() {
        setupNavigateKeyRecord(client);
        Map<String, Set<Object>> actual = client.grab("friends", 1L);
        Assert.assertEquals(client.select(2L), actual);
    }

    @Test
    public void testGrabKeyRecords() {
        setupNavigateKeyRecords(client);
        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        expected.put(1L, client.select(2L));
        expected.put(2L, client.select(4L));
        Map<Long, Map<String, Set<Object>>> actual = client.grab("friends", Lists.newArrayList(1L, 2L));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGrabKeysRecord() {
        setupNavigateKeysRecord(client);

        Map<String, Set<Object>> expected = Stream.of(client.select(2L), client.select(3L))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) ->
                        {
                            Set<Object> newSet = Sets.newSet();
                            newSet.addAll(left);
                            newSet.addAll(right);
                            return newSet;
                        }));

        Map<String, Set<Object>> actual = client.grab(Lists.newArrayList("friends", "family"), 1L);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGrabKeysRecords() {
        setupNavigateKeysRecords(client);
        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        expected.put(1L, client.select(2L));
        expected.put(2L, client.select(4L));

        Map<Long, Map<String, Set<Object>>> actual = client.grab(Lists.newArrayList(
                "friends", "family"), Lists.newArrayList(1L, 2L));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGatherKeyRecord() {
        setupNavigateKeyRecord(client);
        Map<String, Set<Object>> actual = client.gather("friends", 1L);
        Assert.assertEquals(client.select(2L), actual);
    }

    @Test
    public void testGatherKeyRecords() {
        setupNavigateKeyRecords(client);

        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        expected.put(1L, client.select(2L));
        expected.put(2L, Stream.of(client.select(3L), client.select(4L))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) ->
                        {
                            Set<Object> newSet = Sets.newSet();
                            newSet.addAll(left);
                            newSet.addAll(right);
                            return newSet;
                        })));

        Map<Long, Map<String, Set<Object>>> actual = client.gather("friends", Lists.newArrayList(1L, 2L));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGatherKeysRecord() {
        setupNavigateKeysRecord(client);

        Map<String, Set<Object>> expected = Stream.of(client.select(2L), client.select(3L))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) ->
                        {
                            Set<Object> newSet = Sets.newSet();
                            newSet.addAll(left);
                            newSet.addAll(right);
                            return newSet;
                        }));

        Map<String, Set<Object>> actual = client.gather(Lists.newArrayList("friends", "family"), 1L);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGatherKeysRecords() {
        setupNavigateKeysRecords(client);

        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        expected.put(1L, client.select(2L));
        expected.put(2L, Stream.of(client.select(3L), client.select(4L))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (left, right) ->
                        {
                            Set<Object> newSet = Sets.newSet();
                            newSet.addAll(left);
                            newSet.addAll(right);
                            return newSet;
                        })));

        Map<Long, Map<String, Set<Object>>> actual = client.gather(Lists.newArrayList("friends", "family"), Lists.newArrayList(1L, 2L));

        Assert.assertEquals(expected, actual);
    }
}
