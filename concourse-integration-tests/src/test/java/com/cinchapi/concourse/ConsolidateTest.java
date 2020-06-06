/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
 * Unit tests for "consolidate" functionality
 *
 * @author Jeff Nelson
 */
public class ConsolidateTest extends ConcourseIntegrationTest {

    @Test
    public void testConsolidateTwoRecordsNoLinks() {
        Map<String, Object> a = ImmutableMap.of("name", "a", "active", true,
                "age", 100, "foo", "foo");
        Map<String, Object> b = ImmutableMap.of("name", "b", "active", true,
                "age", 200, "bar", "bar");
        long ar = client.insert(a);
        long br = client.insert(b);
        Assert.assertTrue(client.consolidate(ar, br));
        Map<String, Set<Object>> ad = client.select(ar);
        Map<String, Set<Object>> bd = client.select(br);
        Assert.assertTrue(bd.isEmpty());
        Assert.assertEquals(ImmutableMap.of("name", ImmutableSet.of("a", "b"),
                "age", ImmutableSet.of(100, 200), "foo", ImmutableSet.of("foo"),
                "active", ImmutableSet.of(true), "bar", ImmutableSet.of("bar")),
                ad);
    }

    @Test
    public void testCannotConsolidateTwoRecordsIfWouldCreateSelfLink() {
        Map<String, Object> a = ImmutableMap.of("name", "a", "active", true,
                "age", 100, "foo", "foo");
        Map<String, Object> b = ImmutableMap.of("name", "b", "active", true,
                "age", 200, "bar", "bar");
        long ar = client.insert(a);
        long br = client.insert(b);
        client.link("friend", ar, br);
        Timestamp timestamp = Timestamp.now();
        Assert.assertFalse(client.consolidate(ar, br));
        Assert.assertEquals(client.select(ar, timestamp), client.select(ar));
        Assert.assertEquals(client.select(br, timestamp), client.select(br));
    }

    @Test
    public void testCannotConsolidateTwoRecordsIfWouldCreateSelfLink2() {
        Map<String, Object> a = ImmutableMap.of("name", "a", "active", true,
                "age", 100, "foo", "foo");
        Map<String, Object> b = ImmutableMap.of("name", "b", "active", true,
                "age", 200, "bar", "bar");
        long ar = client.insert(a);
        long br = client.insert(b);
        client.link("friend", br, ar);
        Timestamp timestamp = Timestamp.now();
        Assert.assertFalse(client.consolidate(ar, br));
        Assert.assertEquals(client.select(ar, timestamp), client.select(ar));
        Assert.assertEquals(client.select(br, timestamp), client.select(br));
    }

}
