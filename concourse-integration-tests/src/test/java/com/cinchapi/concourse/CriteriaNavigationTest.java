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
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Tests for navigation as a first class concept in evaluation keys of a
 * Criteria
 * 
 * @author Javier Lores
 */
public class CriteriaNavigationTest extends ConcourseIntegrationTest {
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

    @Test
    public void testNavigateAsEvaluationKeyWithoutTimestamp() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateEvaluation(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client
                .select(Criteria.where().key("mother.children")
                        .operator(Operator.EQUALS).value(3));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNavigateAsEvaluationKeyWithTimestamp() {
        Map<Long, Map<String, Set<Object>>> expected = setupNavigateEvaluation(
                client);
        Map<Long, Map<String, Set<Object>>> actual = client.select(Criteria
                .where().key("mother.children").operator(Operator.EQUALS)
                .value(3).at(Timestamp.now()));
        Assert.assertEquals(expected, actual);
    }
}
