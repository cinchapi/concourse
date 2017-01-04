/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse;

import java.util.List;
import java.util.ListIterator;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Concourse#select(long)} API method. Basically the idea
 * is to add/remove some data and ensure browse(record) returns the same thing
 * as fetch(describe(record), record)
 * 
 * @author Jeff Nelson
 */
public class BrowseTest extends ConcourseIntegrationTest {

    @Test
    public void testBrowseEmptyRecord() {
        Assert.assertTrue(client.select(1).isEmpty());
    }

    @Test
    public void testBrowseRecordIsSameAsFetchDescribe() {
        long record = Variables.register("record", TestData.getLong());
        List<String> keys = Variables.register("keys",
                Lists.<String> newArrayList());
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            keys.add(TestData.getSimpleString());
        }
        count = TestData.getScaleCount();
        ListIterator<String> lit = keys.listIterator();
        for (int i = 0; i < count; i++) {
            if(!lit.hasNext()) {
                lit = keys.listIterator();
            }
            String key = lit.next();
            Object value = TestData.getObject();
            client.add(key, TestData.getObject(), record);
            Variables.register(key + "_" + i, value);
        }
        Assert.assertEquals(client.select(record),
                client.select(client.describe(record), record));
    }

    @Test
    public void testBrowseRecordIsSameAsFetchDescribeAfterRemoves() {
        long record = TestData.getLong();
        client.add("a", 1, record);
        client.add("a", 2, record);
        client.add("a", 3, record);
        client.add("b", 1, record);
        client.add("b", 2, record);
        client.add("b", 3, record);
        client.remove("a", 2, record);
        Assert.assertEquals(client.select(record),
                client.select(client.describe(record), record));
    }

    @Test
    public void testHistoricalBrowseRecordIsSameAsHistoricalFetchDescribe() {
        long record = TestData.getLong();
        client.add("a", 1, record);
        client.add("a", 2, record);
        client.add("a", 3, record);
        client.add("b", 1, record);
        client.add("b", 2, record);
        client.add("b", 3, record);
        client.remove("a", 2, record);
        Timestamp timestamp = Timestamp.now();
        client.add("c", 1, record);
        client.add("c", 2, record);
        client.add("c", 3, record);
        client.add("d", 1, record);
        client.add("d", 2, record);
        client.add("d", 3, record);
        client.remove("c", 2, record);
        Assert.assertEquals(client.select(record, timestamp), client.select(
                client.describe(record, timestamp), record, timestamp));
    }

}
