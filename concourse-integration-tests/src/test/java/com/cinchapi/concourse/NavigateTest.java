/*
 * Licensed to Cinchapi Inc, under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. Cinchapi Inc. licenses this
 * file to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a
 * copy of the License at
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

import java.util.Iterator;
import java.util.List;
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

    @Test
    public void testNavigateKeyCcl() {
        String key1 = "friends.name";
        client.add("name", "foo", 1);
        client.add("age", 30, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Set<String>> actual = Maps.newHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("hello");
        actual.put((long) 2, set);
        Map<Long, Set<String>> expected = client.navigate(key1, "age < 30");
        System.out.println(expected);
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeyCriteria() {
        String key1 = "friends.name";
        client.add("name", "foo", 1);
        client.add("age", 30, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Set<String>> actual = Maps.newHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("hello");
        actual.put((long) 2, set);
        Map<Long, Set<String>> expected = client.navigate(key1, Criteria.where()
                .key("age").operator(Operator.LESS_THAN).value(30).build());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeyRecords() {
        List<Long> records = Lists.newArrayList();
        records.add((long) 1);
        records.add((long) 2);
        String key1 = "friends.name";
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Set<String>> actual = Maps.newHashMap();
        Set<String> set1 = Sets.newHashSet();
        Set<String> set2 = Sets.newHashSet();
        set1.add("bar");
        set2.add("jeff");
        set2.add("raghav");
        actual.put((long) 1, set1);
        actual.put((long) 2, set2);
        Map<Long, Set<String>> expected = client.navigate(key1, records);
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeysRecordsTime() {
        List<String> keys = Lists.newArrayList();
        List<Long> records = Lists.newArrayList();
        records.add((long) 1);
        records.add((long) 2);
        String key1 = "friends.name";
        String key2 = "friends.friends.name";
        keys.add(key1);
        keys.add(key2);
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Map<String, Set<String>>> actual = Maps.newHashMap();
        Map<String, Set<String>> map1 = Maps.newHashMap();
        Map<String, Set<String>> map2 = Maps.newHashMap();
        Set<String> set1 = Sets.newHashSet();
        set1.add("bar");
        Set<String> set2 = Sets.newHashSet();
        set2.add("jeff");
        set2.add("raghav");
        map1.put("name", set1);
        map1.put("name", set2);
        set1 = Sets.newHashSet();
        set2 = Sets.newHashSet();
        set1.add("jeff");
        set1.add("raghav");
        map2.put("name", set1);
        map2.put("name", set2);
        actual.put((long) 1, map1);
        actual.put((long) 2, map2);
        Map<Long, Map<String, Set<String>>> expected = client.navigate(keys,
                records, Timestamp.now());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeysRecordTime() {
        List<String> keys = Lists.newArrayList();
        String key1 = "friends.name";
        String key2 = "friends.friends.name";
        keys.add(key1);
        keys.add(key2);
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<String, Set<String>> actual = Maps.newHashMap();
        Set<String> set1 = Sets.newHashSet();
        set1.add("bar");
        Set<String> set2 = Sets.newHashSet();
        set2.add("jeff");
        set2.add("raghav");
        actual.put("name", set1);
        actual.put("name", set2);
        Map<String, Set<String>> expected = client.navigate(keys, 1,
                Timestamp.now());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeysCclTime() {
        List<String> keys = Lists.newArrayList();
        String key1 = "friends.name";
        String key2 = "friends.friends.name";
        keys.add(key1);
        keys.add(key2);
        client.add("name", "foo", 1);
        client.add("age", 35, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Map<String, Set<String>>> actual = Maps.newHashMap();
        Map<String, Set<String>> map = Maps.newHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("bar");
        map.put("name", set);
        set = Sets.newHashSet();
        set.add("hello");
        map.put("name", set);
        actual.put((long) 1, map);
        Map<Long, Map<String, Set<String>>> expected = client.navigate(keys,
                "age > 30", Timestamp.now());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeyRecordTime() {
        String key = "friends.name";
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        Map<Long, Set<String>> actual = Maps.newLinkedHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("bar");
        actual.put((long) 2, set);
        Map<Long, Set<String>> expected = client.navigate(key, 1,
                Timestamp.now());
        actual.forEach((k, value) -> {
            Iterator<String> it1 = actual.get(k).iterator();
            while (it1.hasNext()) {
                String str = it1.next();
                Assert.assertTrue(expected.get(k).contains(str));
            }
            Assert.assertEquals(actual.get(k).size(), expected.get(k).size());
        });
    }

    @Test
    public void testNavigateKeyRecordsTime() {
        List<Long> records = Lists.newArrayList();
        records.add((long) 1);
        records.add((long) 2);
        String key1 = "friends.name";
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Set<String>> actual = Maps.newHashMap();
        Set<String> set1 = Sets.newHashSet();
        Set<String> set2 = Sets.newHashSet();
        set1.add("bar");
        set2.add("jeff");
        set2.add("raghav");
        actual.put((long) 1, set1);
        actual.put((long) 2, set2);
        Map<Long, Set<String>> expected = client.navigate(key1, records,
                Timestamp.now());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeyCclTime() {
        String key1 = "friends.name";
        client.add("name", "foo", 1);
        client.add("age", 30, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Set<String>> actual = Maps.newHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("hello");
        actual.put((long) 2, set);
        Map<Long, Set<String>> expected = client.navigate(key1, "age < 30",
                Timestamp.now());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeyCriteriaTime() {
        String key1 = "friends.name";
        client.add("name", "foo", 1);
        client.add("age", 30, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Set<String>> actual = Maps.newHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("hello");
        actual.put((long) 2, set);
        Map<Long, Set<String>> expected = client
                .navigate(
                        key1, Criteria.where().key("age")
                                .operator(Operator.LESS_THAN).value(30).build(),
                        Timestamp.now());
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeysRecords() {
        List<String> keys = Lists.newArrayList();
        List<Long> records = Lists.newArrayList();
        records.add((long) 1);
        records.add((long) 2);
        String key1 = "friends.name";
        String key2 = "friends.friends.name";
        keys.add(key1);
        keys.add(key2);
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<Long, Map<String, Set<String>>> actual = Maps.newHashMap();
        Map<String, Set<String>> map1 = Maps.newHashMap();
        Map<String, Set<String>> map2 = Maps.newHashMap();
        Set<String> set1 = Sets.newHashSet();
        set1.add("bar");
        Set<String> set2 = Sets.newHashSet();
        set2.add("jeff");
        set2.add("raghav");
        map1.put("name", set1);
        map1.put("name", set2);
        set1 = Sets.newHashSet();
        set2 = Sets.newHashSet();
        set1.add("jeff");
        set1.add("raghav");
        map2.put("name", set1);
        map2.put("name", set2);
        actual.put((long) 1, map1);
        actual.put((long) 2, map2);
        Map<Long, Map<String, Set<String>>> expected = client.navigate(keys,
                records);
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeysRecord() {
        List<String> keys = Lists.newArrayList();
        String key1 = "friends.name";
        String key2 = "friends.friends.name";
        keys.add(key1);
        keys.add(key2);
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("friends", Link.to(3), 2);
        client.add("friends", Link.to(4), 2);
        client.add("name", "raghav", 3);
        client.add("name", "jeff", 4);
        Map<String, Set<String>> actual = Maps.newHashMap();
        Set<String> set1 = Sets.newHashSet();
        set1.add("bar");
        Set<String> set2 = Sets.newHashSet();
        set2.add("jeff");
        set2.add("raghav");
        actual.put("name", set1);
        actual.put("name", set2);
        Map<String, Set<String>> expected = client.navigate(keys, 1);
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeysCcl() {
        List<String> keys = Lists.newArrayList();
        String key1 = "friends.name";
        String key2 = "friends.friends.name";
        keys.add(key1);
        keys.add(key2);
        client.add("name", "foo", 1);
        client.add("age", 35, 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        client.add("age", 20, 2);
        client.add("friends", Link.to(3), 2);
        client.add("name", "hello", 3);
        Map<Long, Map<String, Set<String>>> actual = Maps.newHashMap();
        Map<String, Set<String>> map = Maps.newHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("bar");
        map.put("name", set);
        set = Sets.newHashSet();
        set.add("hello");
        map.put("name", set);
        actual.put((long) 1, map);
        Map<Long, Map<String, Set<String>>> expected = client.navigate(keys,
                "age > 30");
        actual.forEach((key, value) -> {
            Assert.assertEquals(expected.get(key), actual.get(key));
        });
    }

    @Test
    public void testNavigateKeyRecord() {
        String key = "friends.name";
        client.add("name", "foo", 1);
        client.add("friends", Link.to(2), 1);
        client.add("name", "bar", 2);
        Map<Long, Set<String>> actual = Maps.newLinkedHashMap();
        Set<String> set = Sets.newHashSet();
        set.add("bar");
        actual.put((long)2, set);
        Map<Long, Set<String>> expected = client.navigate(key, 1);
        actual.forEach((k, value) -> {
            Iterator<String> it1 = expected.get(k).iterator();
            while (it1.hasNext()) {
                String str = it1.next();
                Assert.assertTrue(actual.get(k).contains(str));
            }
            Assert.assertEquals(actual.get(k).size(), expected.get(k).size());
        });
    }

}
