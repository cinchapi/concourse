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
import org.junit.Ignore;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Numbers;

/**
 * Tests to check the functionality of average feature.
 * 
 * @author Raghav Babu
 */
public class CalculateAverageTest extends ConcourseIntegrationTest {

    @Test
    public void testAverageKey() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 64;
        Number expected = client.calculate().average(key);
        Assert.assertEquals(expected.intValue(), actual / 3);
    }

    @Test
    public void testAverageKeyCcl() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        Number expected = client.calculate().average(key, "name = bar");
        Assert.assertTrue(Numbers.areEqual(expected, actual / 2));
    }

    @Test
    public void testAverageKeyCclTime() {
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
        Number expected = client.calculate().average(key, "name = bar",
                timestamp);
        Assert.assertTrue(Numbers.areEqual(expected, actual / 2));
    }

    @Test
    @Ignore
    public void testAverageKeyCclTimestr() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        // Timestamp timestamp = Timestamp.fromString("1 second ago");
        // NOTE: Replaced the above timestamp because the logic of the test is
        // fundamentally flawed. The above will return a relative timestamp that
        // is resolved by the server. Therefore, the preciseness of the
        // resolution is subject to latency between message passing between the
        // client and server. In general, we need to get rid of all Timestr
        // methods because they suffer from this.
        Timestamp timestamp = client.time("1 second ago");
        Threads.sleep(1000);
        client.add(key, 100, 2);
        Number expected = client.calculate().average(key, "name = bar",
                timestamp);
        Assert.assertTrue(Numbers.areEqual(expected, actual / 2));
    }

    @Test
    public void testAverageKeyCriteria() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 19, 2);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        int actual = 34;
        Number expected = client.calculate().average(key, Criteria.where()
                .key("age").operator(Operator.LESS_THAN).value(20).build());
        Assert.assertTrue(Numbers.areEqual(expected, actual / 2));
    }

    @Test
    public void testAverageKeyCriteriaTime() {
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
                .average(
                        key, Criteria.where().key("age")
                                .operator(Operator.LESS_THAN).value(20).build(),
                        timestamp);
        Assert.assertTrue(Numbers.areEqual(expected, actual / 2));
    }

    @Test(expected = RuntimeException.class)
    public void testAverageKeyException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.calculate().average(key);
    }

    @Test
    public void testAverageKeyRecord() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 19, 1);
        int actual = 49;
        Number expected = client.calculate().average(key, 1);
        Assert.assertTrue(
                Numbers.areEqual(expected, Numbers.divide(actual, 2)));
    }

    @Test(expected = RuntimeException.class)
    public void testAverageKeyRecordException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.calculate().average(key, 1);
    }

    @Test
    public void testAverageKeyRecords() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        int actual = 50;
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        Number expected = client.calculate().average(key, list);
        Assert.assertTrue(Numbers.areEqual(expected, actual / 2));
    }

    @Test(expected = RuntimeException.class)
    public void testAverageKeyRecordsException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        List<Long> list = Lists.newArrayList();
        list.add((long) 1);
        list.add((long) 2);
        client.calculate().average(key, list);
    }

    @Test
    public void testAverageKeyRecordsTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, 20, 2);
        int actual = 50;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        Number expected = client.calculate().average(key,
                Lists.newArrayList(1L, 2L), timestamp);
        Assert.assertEquals(expected.intValue(), actual / 2);
    }

    @Test(expected = RuntimeException.class)
    public void testAverageKeyRecordsTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add("name", "bar", 2);
        client.add(key, "fifty", 2);
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100, 2);
        client.calculate().average(key, Lists.newArrayList(1L, 2L), timestamp);
    }

    @Test
    public void testAverageKeyRecordTime() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, 19, 1);
        int actual = 49;
        Timestamp timestamp = Timestamp.now();
        client.add(key, 100);
        Number expected = client.calculate().average(key, 1, timestamp);
        Assert.assertTrue(
                Numbers.areEqual(expected, Numbers.divide(actual, 2)));
    }

    @Test
    public void testAverageKeyTime() {
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
        Number expected = client.calculate().average(key, timestamp);
        Assert.assertTrue(
                Numbers.areEqual(expected, Numbers.divide(actual, 3)));
    }

    @Test(expected = RuntimeException.class)
    public void testAverageKeyTimeException() {
        String key = "age";
        client.add("name", "foo", 1);
        client.add(key, 30, 1);
        client.add(key, "fifteen", 1);
        client.add("name", "bar", 2);
        client.add(key, 15, 2);
        client.calculate().average(key, Timestamp.now());
    }

    @Test
    public void testAverageKeyReproA() {
        String key = "age";
        client.add("age", 1, 1);
        client.add("age", 2, 2);
        client.add("age", 17, 3);
        client.add("age", 17, 4);
        Number expected = Numbers.divide(1 + 2 + 17 + 17, 4);
        Number actual = client.calculate().average(key);
        Assert.assertTrue(Numbers.areEqual(expected, actual));
    }
}
