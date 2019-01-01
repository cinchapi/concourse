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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests to check the functionality of sum feature.
 * 
 * @author Raghav Babu
 */
public class CalculateSumTest extends ConcourseIntegrationTest {

    @Test
    public void testSumKey() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 64;
        Number expected = client.calculate().sum(key);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test
    public void testSumKeyCcl() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        Number expected = client.calculate().sum(key, "name = bar");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSumKeyCclTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().sum(key, "name = bar", timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSumKeyCriteria() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        Number expected = client.calculate().sum(key, Criteria.where()
                .key("age").operator(Operator.LESS_THAN).value(20).build());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSumKeyCriteriaTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate()
                .sum(key, Criteria.where().key("age")
                        .operator(Operator.LESS_THAN).value(20).build(),
                        timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testSumKeyException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.calculate().sum(key);
    }

    @Test
    public void testSumKeyRecord() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 19, 1);
        int actual = 49;
        Number expected = client.calculate().sum(key, 1);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testSumKeyRecordException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.calculate().sum(key, 1);
    }

    @Test
    public void testSumKeyRecords() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        int actual = 50;
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        Number expected = client.calculate().sum(key, list);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testSumKeyRecordsException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.calculate().sum(key, list);
    }

    @Test
    public void testSumKeyRecordsTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        int actual = 50;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().sum(key,
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test(expected = RuntimeException.class)
    public void testSumKeyRecordsTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        client.calculate().sum(key, Lists.newArrayList(1L, 2L), timestamp);
    }

    @Test
    public void testSumKeyRecordTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 19, 1);
        int actual = 49;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100);
        Number expected = client.calculate().sum(key, 1, timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSumKeyTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 64;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().sum(key, timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testSumKeyTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.calculate().sum(key, Timestamp.now());
    }
}
