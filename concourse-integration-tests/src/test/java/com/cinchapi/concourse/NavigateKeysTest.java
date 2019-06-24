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
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tests for navigation as a first class concept
 */
public class NavigateKeysTest extends ConcourseIntegrationTest {
    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateEvaluation(
            Concourse client) {
        client.add("name", "john", 1);
        client.add("mother", Link.to(2), 1);

        client.add("name", "leslie", 2);
        client.add("children", 3, 2);

        client.add("name", "doe", 3);
        client.add("mother", Link.to(4), 3);

        client.add("name", "martha", 4);
        client.add("children", 2, 4);

        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row1 = Maps.newHashMap();
        expected.put(1L, row1);
        row1.put("name", Sets.newHashSet("john"));
        row1.put("mother", Sets.newHashSet(Link.to(2)));

        return expected;
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static Set<Long> setupNavigateFindEvaluation(Concourse client) {
        client.add("name", "john", 1);
        client.add("mother", Link.to(2), 1);

        client.add("name", "leslie", 2);
        client.add("children", 3, 2);

        client.add("name", "doe", 3);
        client.add("mother", Link.to(4), 3);

        client.add("name", "martha", 4);
        client.add("children", 2, 4);

        return Sets.newHashSet(3L);
    }

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static Map<Long, Map<String, Set<Object>>> setupNavigateSelectionEvaluation(
            Concourse client) {
        client.add("name", "john", 1);
        client.add("mother", Link.to(2), 1);

        client.add("name", "leslie", 2);
        client.add("children", 3, 2);

        client.add("name", "doe", 3);
        client.add("mother", Link.to(4), 3);

        client.add("name", "martha", 4);
        client.add("children", 2, 4);

        Map<Long, Map<String, Set<Object>>> expected = Maps.newHashMap();
        Map<String, Set<Object>> row1 = Maps.newHashMap();
        expected.put(1L, row1);
        row1.put("name", Sets.newHashSet("john"));
        row1.put("mother.children", Sets.newHashSet(3));

        return expected;
    }

    @Test
    public void testNavigateAsEvaluationKey() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateEvaluation(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client
                .select("mother.children = 3");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateFindAsEvaluationKey() {
        Set<Long> expected = setupNavigateFindEvaluation(client);
        Set<Long> actual = client.find("mother.children", Operator.EQUALS, 2);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateAsSelectionAndEvaluationKey() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateSelectionEvaluation(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client.select(
                Lists.newArrayList("name", "mother.children"),
                "mother.children = 3");
        Assert.assertEquals(expected, actual);
    }
}
