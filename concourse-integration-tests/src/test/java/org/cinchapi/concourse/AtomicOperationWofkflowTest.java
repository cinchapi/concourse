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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.util.Random;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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
 * @author jnelson
 */
public class AtomicOperationWofkflowTest extends ConcourseIntegrationTest {

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
        String key = Variables.register("key", TestData.getString());
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
        Assert.assertTrue(client.fetch(key, record).isEmpty());
    }

    @Test
    public void testInserMultiValuesForKeyFailsIfOneOfTheMappingsExists() {
        long record = client.create();
        Multimap<String, Object> data = Variables.register("data",
                LinkedHashMultimap.<String, Object> create());
        String key = Random.getString();
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
        long record = client.create();
        Multimap<String, Object> data = Variables.register("data",
                LinkedHashMultimap.<String, Object> create());
        String a = Random.getString();
        Boolean b = Random.getBoolean();
        Number c = Random.getNumber();
        data.put("a", a);
        data.put("b", b);
        data.put("c", c);
        String json = Variables.register("json", toJsonString(data));
        client.add("b", b, record);
        Assert.assertFalse(client.insert(json, record));
        Assert.assertFalse(client.verify("a", a, record));
        Assert.assertTrue(client.verify("b", b, record));
        Assert.assertFalse(client.verify("c", c, record));
    }

    @Test
    public void testInsertMultiValuesForKey() {
        long record = client.create();
        Multimap<String, Object> data = Variables.register("data",
                LinkedHashMultimap.<String, Object> create());
        String key = Random.getString();
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
        long record = client.create();
        Multimap<String, Object> data = Variables.register("data",
                LinkedHashMultimap.<String, Object> create());
        String a = Random.getString();
        Boolean b = Random.getBoolean();
        Number c = Random.getNumber();
        data.put("a", a);
        data.put("b", b);
        data.put("c", c);
        String json = Variables.register("json", toJsonString(data));
        Assert.assertTrue(client.insert(json, record));
        Assert.assertTrue(client.verify("a", a, record));
        Assert.assertTrue(client.verify("b", b, record));
        Assert.assertTrue(client.verify("c", c, record));
    }

    // TODO testRevertCompletesEvenIfInterrupted

    @Test
    public void testRevertSanityCheck() {
        String key = Variables.register("key", TestData.getString());
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
        Assert.assertEquals(initValues, client.fetch(key, record));
    }

    // TODO testClearCompletesEvenIfInterrupted

    @Test
    @Ignore("waiting on fix for CON-15")
    public void testSetCompletesEvenIfInterrupted() throws InterruptedException {
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
                client.add("foo", TestData.getPositiveNumber().intValue()
                        % count, 1);
            }

        };
        t1.start();
        latch.await();
        t2.start();

        // wait for threads to finish so that the server isn't stopped
        // prematurely
        t1.join();
        t2.join();

        // TODO assert something

    }

    @Test
    public void testSetSanityCheck() {
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            client.add("foo", i, 1);
        }
        client.set("foo", -1, 1);
        Assert.assertEquals(Sets.newHashSet(-1), client.fetch("foo", 1));
    }

    @Test
    public void testVerifyAndSwapInAbortedTransaction() {
        String key = Variables.register("key", TestData.getString());
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
        Assert.assertTrue(client.fetch(key, record).contains(expected));
        Assert.assertFalse(client.fetch(key, record).contains(replacement));
    }

    @Test
    public void testVerifyAndSwapInCommittedTransaction() {
        String key = Variables.register("key", TestData.getString());
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
        Assert.assertFalse(client.fetch(key, record).contains(expected));
        Assert.assertTrue(client.fetch(key, record).contains(replacement));
    }

    @Test
    public void testVerifyAndSwapMultiValues() {
        String key = Variables.register("key", TestData.getString());
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
        Assert.assertFalse(client.fetch(key, record).contains(expected));
        Assert.assertTrue(client.fetch(key, record).contains(replacement));
    }

    @Test
    public void testVerifyAndSwapNegativeCase() {
        String key = Variables.register("key", TestData.getString());
        Object expected = Variables.register("expected", TestData.getObject());
        Object actual = null;
        while (actual == null || expected.equals(actual)) {
            actual = Variables.register("actual", TestData.getObject());
        }
        long record = Variables.register("record", TestData.getLong());
        Object replacement = null;
        while (replacement == null || expected.equals(replacement)) {
            replacement = Variables.register("replacement",
                    TestData.getObject());
        }
        client.add(key, actual, record);
        Assert.assertFalse(client.verifyAndSwap(key, expected, record,
                replacement));
        Assert.assertFalse(client.fetch(key, record).contains(replacement));
        Assert.assertTrue(client.fetch(key, record).contains(actual));
    }

    @Test
    public void testVerifyAndSwapSanityCheck() {
        String key = Variables.register("key", TestData.getString());
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
        Assert.assertTrue(client.fetch(key, record).contains(replacement));
        Assert.assertFalse(client.fetch(key, record).contains(expected));
    }

    // TODO more insert tests!
}
