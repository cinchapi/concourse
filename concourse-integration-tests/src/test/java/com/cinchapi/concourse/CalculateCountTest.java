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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;

/**
 * Unit tests for the various "count" calculations.
 *
 * @author Raghav Babu
 */
public class CalculateCountTest extends ConcourseIntegrationTest {

    @Test
    public void testCountKey() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        long actual = 37;
        Number expected = client.calculate().count(key);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test
    public void testCountKeyCcl() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        long actual = 26;
        Number expected = client.calculate().count(key, "name = bar");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCountKeyCclTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        client.add(key, 15, 2);
        long actual = 26;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().count(key, "name = bar",
                timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCountKeyCriteria() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        client.add(key, 15, 2);
        long actual = 26;
        Number expected = client.calculate().count(key, Criteria.where()
                .key("name").operator(Operator.EQUALS).value("bar").build());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCountKeyCriteriaTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        client.add(key, 15, 2);
        long actual = 26;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate()
                .count(key, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("bar").build(),
                        timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testCountKeyException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.calculate().count(key);
    }

    @Test
    public void testCountKeyRecord() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 49, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        long actual = 12;
        Number expected = client.calculate().count(key, 1);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testCountKeyRecordException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.calculate().count(key, 1);
    }

    @Test
    public void testCountKeyRecords() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 50, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        long actual = 36;
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        Number expected = client.calculate().count(key, list);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testCountKeyRecordsException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.calculate().count(key, list);
    }

    @Test
    public void testCountKeyRecordsTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 50, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        long actual = 36;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().count(key,
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected.intValue(), actual);
    }

    @Test(expected = RuntimeException.class)
    public void testCountKeyRecordsTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        client.calculate().count(key, Lists.newArrayList(1L, 2L), timestamp);
    }

    @Test
    public void testCountKeyRecordTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 49, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        long actual = 12;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100);
        Number expected = client.calculate().count(key, 1, timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCountKeyTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        for (int i = 0; i < 10; i++) {
            client.add(key, 20 + i, 1);
        }
        client.add("name", "bar", 2);
        for (int i = 0; i < 25; i++) {
            client.add(key, 20 + i, 2);
        }
        client.add(key, 15, 2);
        long actual = 37;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().count(key, timestamp);
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = RuntimeException.class)
    public void testCountKeyTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.calculate().count(key, Timestamp.now());
    }

}