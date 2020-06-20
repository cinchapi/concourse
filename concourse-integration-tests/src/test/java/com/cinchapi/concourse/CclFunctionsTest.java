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
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Tests to check the functionality of function features.
 *
 * @author Jeff Nelson
 */
public class CclFunctionsTest extends ConcourseIntegrationTest {

    /**
     * Setup data for each of the tests.
     *
     * @param client
     */
    private static void setupDatabaseKey(Concourse client) {
        client.add("name", "foo", 1);
        client.add("age", 50, 1);
        client.add("age", 100, 1);
        client.add("name", "bar", 2);
        client.add("age", 25, 2);
        client.add("name", "raghav", 3);
        client.add("age", 48, 3);
        client.add("name", "jeff", 4);
        client.add("age", 40, 4);
    }

    @Test
    public void testFunctionSelectionKey() {
        String key = "age | avg";
        setupDatabaseKey(client);
        Map<Long, Set<Object>> actual = client.select(key, "name = foo");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of(75.0)), actual);
    }

    @Test
    public void testFunctionEvaluationKey() {
        String key = "age";
        setupDatabaseKey(client);
        Map<Long, Set<Object>> actual = client.select(key, "age | avg > 50");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of(50, 100)),
                actual);
    }

    @Test
    public void testFunctionEvaluationValue() {
        String key = "age";
        setupDatabaseKey(client);
        Map<Long, Set<Object>> actual = client.select(key, "age > avg(age)");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of(50, 100)),
                actual);
    }

    @Test
    public void testFunctionEvaluationValueWithRecord() {
        String key = "age";
        setupDatabaseKey(client);
        Map<Long, Set<Object>> actual = client.select(key, "age > avg(age, 3)");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of(50, 100)),
                actual);
    }

    @Test
    public void testFunctionEvaluationValueWithRecords() {
        String key = "age";
        setupDatabaseKey(client);
        Map<Long, Set<Object>> actual = client.select(key,
                "age > avg(age, 1, 2)");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of(50, 100)),
                actual);
    }

    @Test
    public void testFunctionEvaluationValueWithCCL() {
        String key = "age";
        setupDatabaseKey(client);
        Map<Long, Set<Object>> actual = client.select(key,
                "age >= avg(age, name = foo)");
        Assert.assertEquals(ImmutableMap.of(1L, ImmutableSet.of(50, 100)),
                actual);
    }

    @Test
    public void testFindKeyOperatorIndexFunctionEvaluationValue() {
        setupDatabaseKey(client);
        Number avgAge = client.calculate().average("age");
        Set<Long> expected = client.find("age", Operator.GREATER_THAN, avgAge);
        Set<Long> actual = client.find("age", Operator.GREATER_THAN,
                "avg(age)");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testFindKeyOperatorEscapedIndexFunctionEvaluationValue() {
        setupDatabaseKey(client);
        long record = client.add("age", "avg(age)");
        Assert.assertEquals(ImmutableSet.of(record),
                client.find("age", Operator.EQUALS, "'avg(age)'"));
    }

    @Test
    public void testFindCclIndexFunctionEvaluationValue() {
        setupDatabaseKey(client);
        Number avgAge = client.calculate().average("age");
        Set<Long> expected = client.find("age > " + avgAge);
        Set<Long> actual = client.find("age > avg(age)");
        Assert.assertEquals(expected, actual);
        expected = client.find("age < " + avgAge);
        actual = client.find("age < avg(age)");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testFindCclEscapedIndexFunctionEvaluationValue() {
        setupDatabaseKey(client);
        long record = client.add("age", "avg(age)");
        Assert.assertEquals(ImmutableSet.of(record),
                client.find("age = 'avg(age)'"));
        try {
            Assert.assertEquals(ImmutableSet.of(),
                    client.find("age = avg(age)"));
        }
        catch (ParseException e) {
            // This means that avg(age) was interpreted as a function and an
            // attempt was made to perform a calculation.
            Assert.assertTrue(e.getMessage().contains(
                    "Cannot perform a calculation on a non-numeric value"));
        }
    }
}
