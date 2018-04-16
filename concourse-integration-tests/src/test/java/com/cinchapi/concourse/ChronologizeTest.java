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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Tests new API named chronologize which returns a mapping from
 * from each timestamp to each non-empty set of values over time.
 * 
 * @author knd
 *
 */
public class ChronologizeTest extends ConcourseIntegrationTest {

    @Test
    public void testChronologizeRangeSanityCheck() {
        String key = "foo";
        long record = 1;
        client.add(key, 1, record);
        client.add(key, 2, record);
        client.add(key, 3, record);
        Map<Timestamp, Set<Object>> chronology = client.chronologize(key,
                record);
        Timestamp preStart = Iterables.get(chronology.keySet(), 0);
        Timestamp start = Iterables.get(chronology.keySet(), 1);
        chronology = client.chronologize(key, record, start, Timestamp.now());
        assertFalse(chronology.keySet().contains(preStart));
        assertEquals(2, chronology.size());
    }

    @Test
    public void testChronologizeIsEmptyForNonExistingKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        String diffKey = Variables.register("diffKey", null);
        Object diffValue = Variables.register("diffValue",
                TestData.getObject());
        while (diffKey == null || key.equals(diffKey)) {
            diffKey = TestData.getSimpleString();
        }
        client.add(diffKey, diffValue, record);
        assertTrue(client.chronologize(key, record).isEmpty());
    }

    @Test
    public void testChronologizeWhenNoRemovalHasHappened() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record);
        assertEquals(testSize, result.size());
        Iterator<Map.Entry<Timestamp, Set<Object>>> setIter = result.entrySet()
                .iterator();
        for (int i = 0; i < testSize; i++) {
            assertEquals(i + 1, setIter.next().getValue().size());
        }
    }

    @Test
    public void testChronologizeWhenRemovalHasHappened() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Object> listOfValues = new ArrayList<Object>();
        Map<Timestamp, Set<Object>> result = null;
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            listOfValues.add(value);
            client.add(key, value, record);
        }
        int expectedMapSize = testSize;
        int expectedLastSetSize = testSize;
        Set<Object> lastValueSet = null;
        // remove 1 value
        expectedMapSize += 1;
        expectedLastSetSize -= 1;
        client.remove(key, listOfValues.get(2), record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables
                .getLast((Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());

        // remove 2 values
        expectedMapSize += 2;
        expectedLastSetSize -= 2;
        client.remove(key, listOfValues.get(0), record);
        client.remove(key, listOfValues.get(4), record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables
                .getLast((Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());

        // add 1 value
        expectedMapSize += 1;
        expectedLastSetSize += 1;
        client.add(key, listOfValues.get(2), record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables
                .getLast((Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());

        // clear all values
        expectedMapSize += expectedLastSetSize - 1; // last empty set filtered
                                                    // out
        expectedLastSetSize = 1;
        client.clear(key, record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables
                .getLast((Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());
    }

    @Test
    public void testChronologizeWhenRemovalHasHappenedWithEmptyValues() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Object> listOfValues = new ArrayList<Object>();
        Map<Timestamp, Set<Object>> result = null;
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            listOfValues.add(value);
            client.add(key, value, record);
        }
        client.remove(key, listOfValues.get(2), record);
        client.remove(key, listOfValues.get(0), record);
        client.remove(key, listOfValues.get(4), record);
        client.add(key, listOfValues.get(2), record);
        client.set(key, listOfValues.get(0), record);
        client.add(key, listOfValues.get(1), record);
        client.add(key, listOfValues.get(2), record);
        client.set(key, listOfValues.get(3), record);
        result = client.chronologize(key, record);
        Variables.register("result", result);
        Variables.register("audit", client.audit(key, record));
        assertEquals(17, result.size());
        assertEquals(5, Iterables.get(result.entrySet(), 4).getValue().size());
        assertEquals(4, Iterables.get(result.entrySet(), 5).getValue().size());
        assertEquals(3, Iterables.get(result.entrySet(), 6).getValue().size());
        assertEquals(2, Iterables.get(result.entrySet(), 7).getValue().size());
        assertEquals(3, Iterables.get(result.entrySet(), 8).getValue().size());
        assertEquals(2, Iterables.get(result.entrySet(), 9).getValue().size());
        assertEquals(1, Iterables.get(result.entrySet(), 10).getValue().size());
        assertEquals(1, Iterables.get(result.entrySet(), 11).getValue().size());
        assertEquals(2, Iterables.get(result.entrySet(), 12).getValue().size());
        assertEquals(3, Iterables.get(result.entrySet(), 13).getValue().size());
        assertEquals(2, Iterables.get(result.entrySet(), 14).getValue().size());
        assertEquals(1, Iterables.get(result.entrySet(), 15).getValue().size());
        assertEquals(1, Iterables.get(result.entrySet(), 16).getValue().size());
    }

    @Test
    public void testChronologizeIsNotAffectedByAddingValueAlreadyInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Object> listOfValues = new ArrayList<Object>();
        Map<Timestamp, Set<Object>> result = null;
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            listOfValues.add(value);
            client.add(key, value, record);
        }
        int expectedMapSize = testSize;
        int expectedLastSetSize = testSize;
        Set<Object> lastValueSet = null;
        // add 1 already existed value
        client.add(key, listOfValues.get(2), record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables
                .getLast((Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());
    }

    @Test
    public void testChronologizeIsNotAffectedByRemovingValueNotInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        Map<Timestamp, Set<Object>> result = null;
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        int expectedMapSize = testSize;
        int expectedLastSetSize = testSize;
        Set<Object> lastValueSet = null;
        // remove 1 non-existing value
        Object nonValue = null;
        while (nonValue == null || initValues.contains(nonValue)) {
            nonValue = TestData.getObject();
        }
        client.remove(key, nonValue, record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables
                .getLast((Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());
    }

    @Test
    public void testChronologizeHasFilteredOutEmptyValueSets() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        Map<Timestamp, Set<Object>> result = null;
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.set(key, value, record);
        }
        result = client.chronologize(key, record);
        for (Set<Object> values : result.values()) {
            assertFalse(values.isEmpty());
        }
    }

    @Test
    public void testChronologizeWithStartTimestampAndEndTimestampBeforeAnyValuesChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        Timestamp startTimestamp = Variables.register("startTimestamp",
                Timestamp.now());

        Timestamp endTimestamp = Variables.register("endTimestamp",
                Timestamp.now());

        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                startTimestamp, endTimestamp);
        result = Variables.register("result", result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testChronologizeWithStartTimestampBeforeAndEndTimestampAfterAnyValuesChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        Timestamp startTimestamp = Variables.register("startTimestamp",
                Timestamp.now());

        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Timestamp endTimestamp = Variables.register("endTimestamp",
                Timestamp.now());

        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                startTimestamp, endTimestamp);
        Set<Object> lastResultSet = Iterables.getLast(result.values());
        result = Variables.register("result", result);
        assertEquals(testSize, result.size());
        assertEquals(testSize, lastResultSet.size());
    }

    @Test
    public void testChronologizeWithStartTimestampAndEndTimestampAfterAnyValuesChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Timestamp startTimestamp = Variables.register("startTimestamp",
                Timestamp.now());

        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Timestamp endTimestamp = Variables.register("endTimestamp",
                Timestamp.now());

        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                startTimestamp, endTimestamp);
        Set<Object> lastResultSet = Iterables.getLast(result.values());
        assertEquals(testSize, result.size());
        assertEquals(testSize * 2, lastResultSet.size());
    }

    @Test
    public void testChronolgizeWithStartTimestampAsEpochAndEndTimestampAsNowInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        client.set(key, TestData.getObject(), record);
        Timestamp epoch = Variables.register("epochTimestamp",
                Timestamp.epoch());
        Timestamp now = Variables.register("nowTimestamp", Timestamp.now());
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                epoch, now);
        result = Variables.register("result", result);
        Set<Object> lastResultSet = Iterables.getLast(result.values());
        assertEquals(testSize * 2, result.size());
        assertEquals(1, lastResultSet.size());
    }

    @Test
    public void testChronologizeWithEndTimestampIsExclusiveAtExactFirstValueChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Timestamp> timestamps = new ArrayList<Timestamp>();
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
            timestamps.add(Timestamp.now());

        }
        Map<Timestamp, Set<Object>> chronology = client.chronologize(key,
                record);
        Timestamp exactStartTimestamp = Variables
                .register("exactStartTimestamp", Iterables.getFirst(
                        (Iterable<Timestamp>) chronology.keySet(), null));
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                Timestamp.epoch(), exactStartTimestamp);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testChronologizeWithEndTimestampIsExclusiveAfterFirstValueChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Timestamp> timestamps = new ArrayList<Timestamp>();
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
            timestamps.add(Timestamp.now());

        }
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                Timestamp.epoch(), timestamps.get(0));
        Set<Object> lastResultSet = Iterables.getLast(result.values());
        result = Variables.register("result", result);
        assertEquals(1, result.size());
        assertEquals(1, lastResultSet.size());
    }

    @Test
    public void testChronologizeWithStartTimestampIsInclusiveAtExactFirstValueChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Timestamp> timestamps = new ArrayList<Timestamp>();
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
            timestamps.add(Timestamp.now());

        }
        Map<Timestamp, Set<Object>> chronology = client.chronologize(key,
                record);
        Timestamp exactStartTimestamp = Variables
                .register("exactStartTimestamp", Iterables.getFirst(
                        (Iterable<Timestamp>) chronology.keySet(), null));
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                exactStartTimestamp, timestamps.get(0));
        Set<Object> lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(1, lastResultSet.size());
    }

    @Test
    public void testChronologizeWithStartTimestampIsInclusiveAtExactLastValueChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Timestamp> timestamps = new ArrayList<Timestamp>();
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
            timestamps.add(Timestamp.now());

        }
        Map<Timestamp, Set<Object>> chronologie = client.chronologize(key,
                record);
        Timestamp exactEndTimestamp = Variables.register("exactEndTimestamp",
                Iterables.getLast((Iterable<Timestamp>) chronologie.keySet()));
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                exactEndTimestamp, Timestamp.now());
        Set<Object> lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(testSize, lastResultSet.size());
    }

    @Test
    public void testChronologizeWithStartTimestampEqualsEndTimestampBeforeFirstValueChangeInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Timestamp> timestamps = new ArrayList<Timestamp>();
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
            timestamps.add(Timestamp.now());

        }
        // check same timestamps before initial add
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record,
                Timestamp.epoch(), Timestamp.epoch());
        assertTrue(result.isEmpty());
    }

    @Test
    public void testChronologizeWithStartTimestampGreaterThanEndTimestampInKeyInRecord() {
        long record = Variables.register("record", Time.now());
        String key = Variables.register("key", TestData.getSimpleString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues",
                Sets.newHashSet());
        List<Timestamp> timestamps = new ArrayList<Timestamp>();
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
            timestamps.add(Timestamp.now());

        }
        client.chronologize(key, record, timestamps.get(3), timestamps.get(2));
    }

}
