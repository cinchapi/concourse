/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
 * Unit tests for "consolidate" functionality.
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

    @Test
    public void testConsolidateTwoRecordsReplaceIncomingLinks() {
        long a = client.add("name", "a");
        long b = client.add("name", "b");
        long c = client.add("name", "c");
        long d = client.add("name", "d");
        long e = client.add("name", "e");
        client.link("friends", c, a);
        client.link("friends", e, b);
        client.link("friends", b, c);
        client.link("friends", a, d);
        client.link("friends", d, e);
        Map<String, Set<Long>> aincoming1 = client.trace(a);
        Assert.assertFalse(aincoming1.isEmpty());
        Assert.assertTrue(client.consolidate(b, a));
        Map<String, Set<Long>> aincoming2 = client.trace(a);
        Assert.assertTrue(aincoming2.isEmpty());
        Assert.assertEquals(ImmutableMap.of("friends", ImmutableSet.of(d, c)),
                client.trace(b));
        Assert.assertTrue(client.select(a).isEmpty());
        Assert.assertEquals(ImmutableMap.of("friends",
                ImmutableSet.of(Link.to(e), Link.to(c)), "name",
                ImmutableSet.of("b", "a")), client.select(b));
    }

    @Test
    public void testCannotConsolidateTwoRecordsIfWouldCreateSelfLinkWithinGraph() {
        long a = client.add("name", "a");
        long b = client.add("name", "b");
        long c = client.add("name", "c");
        long d = client.add("name", "d");
        long e = client.add("name", "e");
        client.link("friends", c, a);
        client.link("friends", e, b);
        client.link("friends", b, c);
        client.link("friends", a, d);
        client.link("friends", d, e);
        Assert.assertFalse(client.consolidate(a, d));
    }

    @Test
    public void testConsolidateMultipleRecords() {
        long a = client.add("name", "a");
        long b = client.add("name", "b");
        long c = client.add("name", "c");
        long d = client.add("name", "d");
        long e = client.add("name", "e");
        client.link("friends", 1, d);
        client.add("friends", 1, e);
        client.link("friends", 1, c);
        client.link("friends", 5, c);
        Assert.assertTrue(client.consolidate(a, c, d, e, a, b, c));
        Assert.assertEquals(
                ImmutableMap.of("friends",
                        ImmutableSet.<Object> of(Link.to(1), Link.to(5), 1),
                        "name", ImmutableSet.of("a", "c", "d", "e", "b")),
                client.select(a));
    }

}
