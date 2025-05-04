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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for "trace" functionality in Concourse
 *
 * @author Jeff Nelson
 */
public class TraceTest extends ConcourseIntegrationTest {

    @Test
    public void testTraceRecord() {
        client.link("foo", ImmutableList.of(1L, 2L), 3);
        client.add("bar", Link.to(2), 4);
        client.add("baz", false, 3);
        client.link("bang", 2, 5);
        Map<String, Set<Long>> incoming = client.trace(2);
        Assert.assertEquals(
                ImmutableMap.of("foo", ImmutableSet.of(3L), "bar",
                        ImmutableSet.of(4L), "bang", ImmutableSet.of(5L)),
                incoming);
    }

    @Test
    public void testTraceRecordTime() {
        client.link("foo", ImmutableList.of(1L, 2L), 3);
        client.add("bar", Link.to(2), 4);
        client.add("baz", false, 3);
        client.link("bang", 2, 5);
        Timestamp timestamp = Timestamp.now();
        client.unlink("foo", 2, 3);
        Map<String, Set<Long>> incoming = client.trace(2, timestamp);
        Assert.assertEquals(
                ImmutableMap.of("foo", ImmutableSet.of(3L), "bar",
                        ImmutableSet.of(4L), "bang", ImmutableSet.of(5L)),
                incoming);
    }

    @Test
    public void testTraceRecords() {
        client.link("foo", ImmutableList.of(1L, 2L), 3);
        client.add("bar", Link.to(2), 4);
        client.add("baz", false, 3);
        client.link("bang", 2, 5);
        Map<Long, Map<String, Set<Long>>> incoming = client
                .trace(ImmutableList.of(1L, 2L));
        Assert.assertEquals(ImmutableMap.of(1L,
                ImmutableMap.of("foo", ImmutableSet.of(3L)), 2L,
                ImmutableMap.of("foo", ImmutableSet.of(3L), "bar",
                        ImmutableSet.of(4L), "bang", ImmutableSet.of(5L))),
                incoming);
    }

    @Test
    public void testTraceRecordsTime() {
        client.link("foo", ImmutableList.of(1L, 2L), 3);
        client.add("bar", Link.to(2), 4);
        client.add("baz", false, 3);
        client.link("bang", 2, 5);
        Timestamp timestamp = Timestamp.now();
        client.unlink("foo", 1, 3);
        client.unlink("foo", 2, 3);
        Map<Long, Map<String, Set<Long>>> incoming = client
                .trace(ImmutableList.of(1L, 2L), timestamp);
        Assert.assertEquals(ImmutableMap.of(1L,
                ImmutableMap.of("foo", ImmutableSet.of(3L)), 2L,
                ImmutableMap.of("foo", ImmutableSet.of(3L), "bar",
                        ImmutableSet.of(4L), "bang", ImmutableSet.of(5L))),
                incoming);

    }

}
