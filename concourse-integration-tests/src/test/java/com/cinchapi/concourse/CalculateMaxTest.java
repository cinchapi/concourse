/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;

/**
 * Unit tests for the "max" aggregation function.
 *
 * @author Jeff Nelson
 * @author Raghav Babu
 */
public class CalculateMaxTest extends ConcourseIntegrationTest {

    @Test
    public void testMaxKey() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 49, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 49;
        Number expected = client.calculate().max(key);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test
    public void testMaxKeyCcl() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 19;
        Number expected = client.calculate().max(key, "name = bar");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMaxKeyCclTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 19;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().max(key, "name = bar", timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMaxKeyCriteria() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 19;
        Number expected = client.calculate().max(key, Criteria.where()
                .key("age").operator(Operator.LESS_THAN).value(20).build());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMaxKeyCriteriaTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 19;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate()
                .max(key, Criteria.where().key("age")
                        .operator(Operator.LESS_THAN).value(20).build(),
                        timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMaxKeyRecord() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 49, 1);
        int actual = 49;
        Number expected = client.calculate().max(key, 1);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testMaxKeyRecordException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.calculate().max(key, 1);
    }

    @Test
    public void testMaxKeyRecords() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 50, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        int actual = 50;
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        Number expected = client.calculate().max(key, list);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testMaxKeyRecordsException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.calculate().max(key, list);
    }

    @Test
    public void testMaxKeyRecordsTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 50, 2);
        int actual = 50;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().max(key,
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test(expected = RuntimeException.class)
    public void testMaxKeyRecordsTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        client.calculate().max(key, Lists.newArrayList(1L, 2L), timestamp);
    }

    @Test
    public void testMaxKeyRecordTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 49, 1);
        int actual = 49;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100);
        Number expected = client.calculate().max(key, 1, timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMaxKeyTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 20, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add(key, 45, 2);
        int actual = 45;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().max(key, timestamp);
        Assert.assertEquals(expected, actual);
    }
}
