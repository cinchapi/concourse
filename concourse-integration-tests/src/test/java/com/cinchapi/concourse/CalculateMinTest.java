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
 * Unit test for "min" calculation functions.
 *
 * @author Jeff Nelson
 * @author Raghav Babu
 */
public class CalculateMinTest extends ConcourseIntegrationTest {

    @Test
    public void testMinKey() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 49, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 15;
        Number expected = client.calculate().min(key);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test
    public void testMinKeyCcl() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 15;
        Number expected = client.calculate().min(key, "name = bar");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMinKeyCclTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 15;
        Timestamp timestamp = Timestamp.now();
        client.add(key, -1, 2);
        Number expected = client.calculate().min(key, "name = bar", timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMinKeyCriteria() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 15;
        Number expected = client.calculate().min(key, Criteria.where()
                .key("age").operator(Operator.LESS_THAN).value(20).build());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMinKeyCriteriaTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 15;
        Timestamp timestamp = Timestamp.now();
        client.add(key, -1, 2);
        Number expected = client.calculate()
                .min(key, Criteria.where().key("age")
                        .operator(Operator.LESS_THAN).value(20).build(),
                        timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMinKeyRecord() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 49, 1);
        int actual = 30;
        Number expected = client.calculate().min(key, 1);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testMinKeyRecordException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.calculate().min(key, 1);
    }

    @Test
    public void testMinKeyRecords() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 50, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        int actual = 20;
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        Number expected = client.calculate().min(key, list);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testMinKeyRecordsException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.calculate().min(key, list);
    }

    @Test
    public void testMinKeyRecordsTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 50, 2);
        int actual = 30;
        Timestamp timestamp = Timestamp.now();
        client.add(key, -1, 2);
        Number expected = client.calculate().min(key,
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test(expected = RuntimeException.class)
    public void testMinKeyRecordsTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        client.calculate().min(key, Lists.newArrayList(1L, 2L), timestamp);
    }

    @Test
    public void testMinKeyRecordTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 49, 1);
        int actual = 30;
        Timestamp timestamp = Timestamp.now();
        client.add(key, -1, 1);
        Number expected = client.calculate().min(key, 1, timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMinKeyTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 20, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add(key, 45, 2);
        int actual = 19;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 1, 2);
        Number expected = client.calculate().min(key, timestamp);
        Assert.assertEquals(expected, actual);
    }
}
