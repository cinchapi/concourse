/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.http;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Response;

/**
 * Unit tests for the audit functionality in the REST API.
 * 
 * @author Jeff Nelson
 */
public class RestAuditTest extends RestTest {

    @Test
    public void testAuditRecord() {
        long record = TestData.getLong();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(TestData.getSimpleString(), i, record);
        }
        Map<Long, String> resp = bodyAsJava(get("/{0}/audit", record),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(record);
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

    @Test
    public void testAuditKeyReturns400Error() {
        Response resp = get("/foo/audit");
        Assert.assertEquals(400, resp.code());
    }

    @Test
    public void testAuditRecordStart() {
        long record = TestData.getLong();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(TestData.getSimpleString(), i, record);
        }
        long start = Time.now();
        for (int i = 0; i < count; i++) {
            client.add(TestData.getSimpleString(), i, record);
        }
        Map<Long, String> resp = bodyAsJava(
                get("/{0}/audit?start={1}", record, start),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(record,
                Timestamp.fromMicros(start));
        Assert.assertEquals(expected.size(), resp.size());
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertTrue(timestamp >= start);
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

    @Test
    public void testAuditRecordStartEnd() {
        long record = TestData.getLong();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(TestData.getSimpleString(), i, record);
        }
        long start = Time.now();
        for (int i = 0; i < count; i++) {
            client.add(TestData.getSimpleString(), i, record);
        }
        long end = Time.now();
        for (int i = 0; i < count; i++) {
            client.add(TestData.getSimpleString(), i, record);
        }
        Map<Long, String> resp = bodyAsJava(
                get("/{0}/audit?start={1}&end={2}", record, start, end),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(record,
                Timestamp.fromMicros(start), Timestamp.fromMicros(end));
        Assert.assertEquals(expected.size(), resp.size());
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertTrue(timestamp >= start);
            Assert.assertTrue(timestamp <= end);
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

    @Test
    public void testAuditKeyRecord() {
        long record = TestData.getLong();
        String key = TestData.getSimpleString();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        Map<Long, String> resp = bodyAsJava(get("/{0}/{1}/audit", key, record),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(key, record);
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

    @Test
    public void testAuditRecordKey() {
        long record = TestData.getLong();
        String key = TestData.getSimpleString();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        Map<Long, String> resp = bodyAsJava(get("/{0}/{1}/audit", record, key),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(key, record);
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

    @Test
    public void testAuditKeyRecordStart() {
        long record = TestData.getLong();
        String key = TestData.getSimpleString();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        long start = Time.now();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        Map<Long, String> resp = bodyAsJava(
                get("/{0}/{1}/audit?start={2}", key, record, start),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(key, record,
                Timestamp.fromMicros(start));
        Assert.assertEquals(expected.size(), resp.size());
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertTrue(timestamp >= start);
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

    @Test
    public void testAuditKeyRecordStartEnd() {
        long record = TestData.getLong();
        String key = TestData.getSimpleString();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        long start = Time.now();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        long end = Time.now();
        for (int i = 0; i < count; i++) {
            client.add(key, i, record);
        }
        Map<Long, String> resp = bodyAsJava(
                get("/{0}/{1}/audit?start={2}&end={3}", key, record, start, end),
                new TypeToken<Map<Long, String>>() {});
        Map<Timestamp, String> expected = client.audit(key, record,
                Timestamp.fromMicros(start), Timestamp.fromMicros(end));
        Assert.assertEquals(expected.size(), resp.size());
        for (Entry<Timestamp, String> entry : expected.entrySet()) {
            long timestamp = entry.getKey().getMicros();
            Assert.assertTrue(timestamp >= start);
            Assert.assertTrue(timestamp <= end);
            Assert.assertEquals(entry.getValue(), resp.get(timestamp));
        }
    }

}
