/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse;

import java.util.List;
import java.util.ListIterator;

import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Concourse#browse(long)} API method. Basically the idea
 * is to add/remove some data and ensure browse(record) returns the same thing
 * as fetch(describe(record), record)
 * 
 * @author jnelson
 */
public class BrowseTest extends ConcourseIntegrationTest {

    @Test
    public void testBrowseEmptyRecord() {
        Assert.assertTrue(client.browse(1).isEmpty());
    }

    @Test
    public void testBrowseRecordIsSameAsFetchDescribe() {
        long record = Variables.register("record", TestData.getLong());
        List<String> keys = Variables.register("keys",
                Lists.<String> newArrayList());
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            keys.add(TestData.getString());
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
        Assert.assertEquals(client.browse(record),
                client.fetch(client.describe(record), record));
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
        Assert.assertEquals(client.browse(record),
                client.fetch(client.describe(record), record));
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
        Assert.assertEquals(client.browse(record, timestamp), client.fetch(
                client.describe(record, timestamp), record, timestamp));
    }

}
