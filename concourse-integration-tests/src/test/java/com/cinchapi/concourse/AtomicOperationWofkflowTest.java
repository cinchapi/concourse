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
package com.cinchapi.concourse;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Tests for atomic operations that are defined in {@link ConcourseServer}.
 * 
 * @author Jeff Nelson
 */
public class AtomicOperationWofkflowTest extends ConcourseIntegrationTest {

    /**
     * Return a random collection of data to be used in a test of the insert
     * function.
     * 
     * @return the data
     */
    private static Multimap<String, Object> getInsertData() {
        Multimap<String, Object> data = LinkedHashMultimap.create();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            String key = TestData.getSimpleString();
            if(Time.now() % 3 == 0) {
                for (int j = 0; j < TestData.getScaleCount() / 4; j++) {
                    // add multiple values
                    data.put(key, TestData.getObject());
                }
            }
            else {
                data.put(key, TestData.getObject());
            }
        }
        return data;
    }

    /**
     * Return a random collection of data that also includes {@code key} and
     * {@code value} to be used in a test of the insert function.
     * 
     * @param key
     * @param value
     * @return the data
     */
    private static Multimap<String, Object> getInsertData(String key,
            Object value) {
        Multimap<String, Object> data = getInsertData();
        data.put(key, value);
        data.putAll(getInsertData());
        return data;
    }

    /**
     * Convert an object to a {@link JsonElement}.
     * 
     * @param object
     * @return the JSON element
     */
    private static JsonElement toJsonElement(Object object) {
        if(object instanceof Double) {
            return new JsonPrimitive(object + "D");
        }
        else if(object instanceof Number) {
            return new JsonPrimitive((Number) object);
        }
        else if(object instanceof Boolean) {
            return new JsonPrimitive((Boolean) object);
        }
        else {
            return new JsonPrimitive(object.toString());
        }
    }

    /**
     * Convert a multimap containing key/value data to a JSON formatted string.
     * 
     * @param data
     * @return the JSON string
     */
    private static String toJsonString(Multimap<String, Object> data) {
        JsonObject object = new JsonObject();
        for (String key : data.keySet()) {
            if(data.get(key).size() > 1) {
                JsonArray array = new JsonArray();
                for (Object value : data.get(key)) {
                    array.add(toJsonElement(value));
                }
                object.add(key, array);
            }
            else if(data.get(key).size() == 1) {
                object.add(key,
                        toJsonElement(Iterables.getOnlyElement(data.get(key))));
            }
        }
        return object.toString();
    }

    @Test
    public void testCannotVerifyAndSwapDuplicateValue() {
        client.add("foo", 1, 1);
        client.add("foo", 2, 1);
        Assert.assertFalse(client.verifyAndSwap("foo", 2, 1, 1));
    }

    @Test
    public void testClearSanityCheck() {
        String key = Variables.register("key", TestData.getSimpleString());
        long record = Variables.register("record", TestData.getLong());
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        for (int i = 0; i < Variables.register("count",
                TestData.getScaleCount()); i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        client.clear(key, record);
        Assert.assertTrue(client.select(key, record).isEmpty());
    }

    @Test
    public void testInserMultiValuesForKeyFailsIfOneOfTheMappingsExists() {
        long record = Time.now();
        Multimap<String, Object> data = Variables.register("data",
                LinkedHashMultimap.<String, Object> create());
        String key = Random.getSimpleString();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            data.put(key, Random.getObject());
        }
        Object v = Random.getObject();
        client.add(key, v, record);
        data.put(key, v);
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            data.put(key, Random.getObject());
        }
        String json = Variables.register("json", toJsonString(data));
        Assert.assertFalse(client.insert(json, record));
    }

    @Test
    public void testInsertFailsIfSomeDataAlreadyExists() {
        long record = Time.now();
        String key0 = TestData.getSimpleString();
        Object value0 = TestData.getObject();
        Multimap<String, Object> data = Variables.register("data",
                getInsertData(key0, value0));
        String json = Variables.register("json", toJsonString(data));
        client.add(key0, value0, record);
        Assert.assertFalse(client.insert(json, record));
        for (String key : data.keySet()) {
            for (Object value : data.get(key)) {
                if(!key.equals(key0) && !value.equals(value0)) {
                    Assert.assertFalse(client.verify(key, value, record));
                }
            }
        }
    }

    @Test
    public void testInsertIntoNewRecordAlwaysSucceeds() {
        Multimap<String, Object> data = Variables.register("data",
                getInsertData());
        String json = Variables.register("json", toJsonString(data));
        long record = client.insert(json).iterator().next();
        for (String key : data.keySet()) {
            for (Object value : data.get(key)) {
                Variables.register("key", key);
                Variables.register("value", value);
                Assert.assertTrue(client.verify(key, value, record));
            }
        }
    }

    @Test
    public void testInsertIntoNewRecordAlwaysSucceedsReproA() {
        Multimap<String, Object> data = Variables.register("data",
                HashMultimap.<String, Object> create());
        data.put(
                "zotcstcgyjmgecajmebeqnmdpjddhlhbvyegkkjbedvrgqosrvqiuxsrhowedzuyxesmxqkncvxghflh",
                "3");
        String json = Variables.register("json", toJsonString(data));
        long record = client.insert(json).iterator().next();
        for (String key : data.keySet()) {
            for (Object value : data.get(key)) {
                Variables.register("key", key);
                Variables.register("value", value);
                Assert.assertTrue(client.verify(key, value, record));
            }
        }
    }

    @Test
    public void testInsertMultiValuesForKey() {
        long record = Time.now();
        Multimap<String, Object> data = Variables.register("data",
                LinkedHashMultimap.<String, Object> create());
        String key = Random.getSimpleString();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            data.put(key, Random.getObject());
        }
        String json = Variables.register("json", toJsonString(data));
        Assert.assertTrue(client.insert(json, record));
        for (Object value : data.get(key)) {
            Assert.assertTrue(client.verify(key, value, record));
        }
    }

    @Test
    public void testInsertSucceedsIfAllDataIsNew() {
        long record = Time.now();
        Multimap<String, Object> data = Variables.register("data",
                getInsertData());
        String json = Variables.register("json", toJsonString(data));
        Assert.assertTrue(client.insert(json, record));
        for (String key : data.keySet()) {
            for (Object value : data.get(key)) {
                Variables.register("key", key);
                Variables.register("value", value);
                Assert.assertTrue(client.verify(key, value, record));
            }
        }
    }

    @Test
    public void testInsertSucceedsIfAllDataIsNewReproA() {
        long record = Time.now();
        Multimap<String, Object> data = Variables.register("data",
                HashMultimap.<String, Object> create());
        data.put("foo", "007");
        String json = Variables.register("json", toJsonString(data));
        Assert.assertTrue(client.insert(json, record));
        for (String key : data.keySet()) {
            for (Object value : data.get(key)) {
                Assert.assertTrue(client.verify(key, value, record));
            }
        }
    }

    @Test(expected = RuntimeException.class)
    public void testInsertFailsForNonJsonString() {
        Assert.assertTrue(client.insert(TestData.getSimpleString()).isEmpty());
    }

    // TODO testRevertCompletesEvenIfInterrupted

    @Test
    public void testRevertSanityCheck() {
        String key = Variables.register("key", TestData.getSimpleString());
        long record = Variables.register("record", TestData.getLong());
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        for (int i = 0; i < Variables.register("count",
                TestData.getScaleCount()); i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Timestamp timestamp = Timestamp.now();
        Set<Object> values = Variables.register("values",
                Sets.newHashSet(initValues));
        for (int i = 0; i < Variables.register("count",
                TestData.getScaleCount()); i++) {
            Object value = null;
            while (value == null || values.contains(value)) {
                value = TestData.getObject();
            }
            values.add(value);
            client.add(key, value, record);
        }
        client.revert(key, record, timestamp);
        Assert.assertEquals(initValues, client.select(key, record));
    }

    // TODO testClearCompletesEvenIfInterrupted

    @Test
    public void testSetCompletesEvenIfInterrupted() throws InterruptedException {
        final Concourse client2 = Concourse.connect(SERVER_HOST, SERVER_PORT,
                "admin", "admin");
        final int count = 100;
        for (int i = 0; i < count; i++) {
            client.add("foo", i, 1);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t1 = new Thread() {

            @Override
            public void run() {
                latch.countDown();
                client.set("foo", -1, 1);
            }

        };

        // Attempt to interrupt the #set operation happening in thread t1
        Thread t2 = new Thread() {

            @Override
            public void run() {
                Assert.assertTrue(client2.add("foo", 1000, 1));
            }

        };
        t1.start();
        latch.await();
        t2.start();

        // wait for threads to finish so that the server isn't stopped
        // prematurely
        t1.join();
        t2.join();

        Assert.assertTrue(client.select("foo", 1).contains(-1)); // this shows
                                                                 // that the
                                                                 // atomic
                                                                 // operation
                                                                 // has retry
                                                                 // logic

    }

    @Test
    public void testSetSanityCheck() {
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            client.add("foo", i, 1);
        }
        client.set("foo", -1, 1);
        Assert.assertEquals(Sets.newHashSet(-1), client.select("foo", 1));
    }

    @Test
    public void testVerifyAndSwapInAbortedTransaction() {
        String key = Variables.register("key", TestData.getSimpleString());
        Object expected = Variables.register("expected", TestData.getObject());
        long record = Variables.register("record", TestData.getLong());
        client.add(key, expected, record);
        client.stage();
        Object replacement = null;
        while (replacement == null || expected.equals(replacement)) {
            replacement = Variables.register("replacement",
                    TestData.getObject());
        }
        client.verifyAndSwap(key, expected, record, replacement);
        client.abort();
        Assert.assertTrue(client.select(key, record).contains(expected));
        Assert.assertFalse(client.select(key, record).contains(replacement));
    }

    @Test
    public void testVerifyAndSwapInCommittedTransaction() {
        String key = Variables.register("key", TestData.getSimpleString());
        Object expected = Variables.register("expected", TestData.getObject());
        long record = Variables.register("record", TestData.getLong());
        client.add(key, expected, record);
        client.stage();
        Object replacement = null;
        while (replacement == null || expected.equals(replacement)) {
            replacement = Variables.register("replacement",
                    TestData.getObject());
        }
        client.verifyAndSwap(key, expected, record, replacement);
        client.commit();
        Assert.assertFalse(client.select(key, record).contains(expected));
        Assert.assertTrue(client.select(key, record).contains(replacement));
    }

    @Test
    public void testVerifyAndSwapMultiValues() {
        String key = Variables.register("key", TestData.getSimpleString());
        long record = Variables.register("record", TestData.getLong());
        HashSet<Object> values = Variables
                .register("values", Sets.newHashSet());
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            Object value = null;
            while (value == null || values.contains(value)) {
                value = TestData.getObject();
            }
            values.add(value);
            client.add(key, value, record);
        }
        Object replacement = null;
        while (replacement == null || values.contains(replacement)) {
            replacement = Variables.register("replacement",
                    TestData.getObject());
        }
        Object expected = Variables.register("expected",
                values.toArray()[TestData.getScaleCount() % values.size()]);
        Assert.assertTrue(client.verifyAndSwap(key, expected, record,
                replacement));
        Assert.assertFalse(client.select(key, record).contains(expected));
        Assert.assertTrue(client.select(key, record).contains(replacement));
    }

    @Test
    public void testVerifyAndSwapNegativeCase() {
        String key = Variables.register("key", TestData.getSimpleString());
        Object expected = Variables.register("expected", TestData.getObject());
        Object actual = null;
        while (actual == null || expected.equals(actual)) {
            actual = Variables.register("actual", TestData.getObject());
        }
        long record = Variables.register("record", TestData.getLong());
        Object replacement = null;
        while (replacement == null || expected.equals(replacement)
                || actual.equals(replacement)) {
            replacement = Variables.register("replacement",
                    TestData.getObject());
        }
        client.add(key, actual, record);
        Assert.assertFalse(client.verifyAndSwap(key, expected, record,
                replacement));
        Assert.assertFalse(client.select(key, record).contains(replacement));
        Assert.assertTrue(client.select(key, record).contains(actual));
    }

    @Test
    public void testVerifyAndSwapSanityCheck() {
        String key = Variables.register("key", TestData.getSimpleString());
        Object expected = Variables.register("expected", TestData.getObject());
        long record = Variables.register("record", TestData.getLong());
        client.add(key, expected, record);
        Object replacement = null;
        while (replacement == null || expected.equals(replacement)) {
            replacement = Variables.register("replacement",
                    TestData.getObject());
        }
        Assert.assertTrue(client.verifyAndSwap(key, expected, record,
                replacement));
        Assert.assertTrue(client.select(key, record).contains(replacement));
        Assert.assertFalse(client.select(key, record).contains(expected));
    }

    @Test
    public void testFindOrAddNotExists() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long record = client.findOrAdd(key, value);
        Assert.assertEquals(value, client.get(key, record));
    }

    @Test
    public void testFindOrAddExists() {
        String key = TestData.getSimpleString();
        Object value = TestData.getObject();
        long existing = TestData.getLong();
        client.add(key, value, existing);
        Assert.assertEquals(existing, client.findOrAdd(key, value));
    }

    @Test
    public void testFindOrInsertCriteriaExists() {
        String key = TestData.getSimpleString();
        int value = 10;
        String json = toJsonString(getInsertData(key, value));
        long existing = TestData.getLong();
        client.insert(json, existing);
        long record = client.findOrInsert(
                Criteria.where().key(key).operator(Operator.GREATER_THAN)
                        .value(5), json);
        Assert.assertEquals(existing, record);
    }

    @Test
    public void testFindOrInsertCclExists() {
        String key = "foo";
        int value = 10;
        String json = toJsonString(getInsertData(key, value));
        long record = TestData.getLong();
        client.insert(json, record);
        Assert.assertEquals(record, client.findOrInsert("foo > 5", json));
    }

    @Test
    public void testFindOrInsertCriteriaNotExists() {
        String key = TestData.getSimpleString();
        int value = 10;
        String json = toJsonString(getInsertData(key, value));
        long record = TestData.getLong();
        client.insert(json, record);
        Assert.assertNotEquals(record, client.findOrInsert(Criteria.where()
                .key(key).operator(Operator.GREATER_THAN).value(11), json));
    }

    @Test
    public void testFindOrInsertCclNotExists() {
        String key = "foo";
        int value = 10;
        String json = toJsonString(getInsertData(key, value));
        long record = TestData.getLong();
        client.insert(json, record);
        Assert.assertNotEquals(record, client.findOrInsert("foo != 10", json));
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindOrAddDuplicateEntry() {
        String key = TestData.getSimpleString();
        int value = TestData.getInt();
        Set<Long> records = Sets.newHashSet();
        while (records.size() < 2) {
            records.add(TestData.getLong());
        }
        client.add(key, value, records);
        client.findOrAdd(key, value);
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindOrInsertCriteriaDuplicateEntry() {
        String key = "foo";
        int value = 10;
        String json = toJsonString(getInsertData(key, value));
        Set<Long> records = Sets.newHashSet();
        while (records.size() < 2) {
            records.add(TestData.getLong());
        }
        client.insert(json, records);
        client.findOrInsert(Criteria.where().key(key).operator(Operator.EQUALS)
                .value(10), json);
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindOrInsertCclDuplicateEntry() {
        String key = "foo";
        int value = 10;
        String json = toJsonString(getInsertData(key, value));
        Set<Long> records = Sets.newHashSet();
        while (records.size() < 2) {
            records.add(TestData.getLong());
        }
        client.insert(json, records);
        client.findOrInsert("foo = 10", json);
    }

    // TODO more insert tests!
}
