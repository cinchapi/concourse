package org.cinchapi.concourse;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Test;

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
    public void testChronologizeIsEmptyForNonExistingKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
        String diffKey = Variables.register("diffKey", null);
        Object diffValue = Variables.register("diffValue", TestData.getObject());
        while (diffKey == null || key.equals(diffKey)) {
            diffKey = TestData.getString();
        }
        client.add(diffKey, diffValue, record);
        assertTrue(client.chronologize(key, record).isEmpty());
    }
    
    @Test
    public void testChronologizeWhenNoRemovalHasHappened() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        Iterator<Map.Entry<Timestamp, Set<Object>>> setIter = result.entrySet().iterator();
        for (int i = 0; i < testSize; i++) {
            assertEquals(i+1, setIter.next().getValue().size());
        }
    }
    
    @Test
    public void testChronologizeWhenRemovalHasHappened() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        lastValueSet = Iterables.getLast(
                (Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());
        
        // remove 2 values
        expectedMapSize += 2;
        expectedLastSetSize -= 2;
        client.remove(key, listOfValues.get(0), record);
        client.remove(key, listOfValues.get(4), record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables.getLast(
                (Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());
        
        // add 1 value
        expectedMapSize += 1;
        expectedLastSetSize += 1;
        client.add(key, listOfValues.get(2), record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables.getLast(
                (Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());
        
        // clear all values
        expectedMapSize += expectedLastSetSize - 1; // last empty set filtered out
        expectedLastSetSize = 1;
        client.clear(key, record);
        result = client.chronologize(key, record);
        assertEquals(expectedMapSize, result.size());
        lastValueSet = Iterables.getLast(
                (Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());    
    }
    
    @Test
    public void testChronologizeIsNotAffectedByAddingValueAlreadyInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        lastValueSet = Iterables.getLast(
                (Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size());  
    }
    
    @Test
    public void testChronologizeIsNotAffectedByRemovingValueNotInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        lastValueSet = Iterables.getLast(
                (Iterable<Set<Object>>) result.values());
        assertEquals(expectedLastSetSize, lastValueSet.size()); 
    }
    
    @Test
    public void testChronologizeHasFilteredOutEmptyValueSets() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues", 
                Sets.newHashSet());
        Timestamp startTimestamp = Variables.register("startTimestamp", Timestamp.now());
        // some delay
        for (int i = 0; i < 100000000; i++);
        Timestamp endTimestamp = Variables.register("endTimestamp", Timestamp.now());
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(
                key, record, startTimestamp, endTimestamp);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testChronologizeWithStartTimestampBeforeAndEndTimestampAfterAnyValuesChangeInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues", 
                Sets.newHashSet());
        Timestamp startTimestamp = Variables.register("startTimestamp", Timestamp.now());
        // some delay
        for (int i = 0; i < 100000000; i++);
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Timestamp endTimestamp = Variables.register("endTimestamp", Timestamp.now());
        // some delay
        for (int i = 0; i < 100000000; i++);
        // add more values
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(
                key, record, startTimestamp, endTimestamp);
        Set<Object> lastResultSet = Iterables.getLast(
                result.values());
        assertEquals(testSize, result.size());
        assertEquals(testSize, lastResultSet.size());
    }
    
    @Test
    public void testChronologizeWithStartTimestampAndEndTimestampAfterAnyValuesChangeInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        Timestamp startTimestamp = Variables.register("startTimestamp", Timestamp.now());
        // some delay
        for (int i = 0; i < 100000000; i++);
        // add more values
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Timestamp endTimestamp = Variables.register("endTimestamp", Timestamp.now());
        // some delay
        for (int i = 0; i < 100000000; i++);
        // add more values
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            client.add(key, value, record);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(
                key, record, startTimestamp, endTimestamp);
        Set<Object> lastResultSet = Iterables.getLast(
                result.values());
        assertEquals(testSize+1, result.size());
        assertEquals(testSize*2, lastResultSet.size());
    }
    
    @Test
    public void testChronolgizeWithStartTimestampAsEpochAndEndTimestampAsNowInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
        // set value 
        client.set(key, TestData.getObject(), record);
        Timestamp epoch = Variables.register("epochTimestamp", Timestamp.epoch());
        Timestamp now = Variables.register("nowTimestamp", Timestamp.now());
        Map<Timestamp, Set<Object>> result = client.chronologize(
                key, record, epoch, now);
        Set<Object> lastResultSet = Iterables.getLast(
                result.values());
        assertEquals(testSize*2, result.size());
        assertEquals(1, lastResultSet.size());
    }
    
    @Test
    public void testChronologizeWithStartTimestampIsInclusiveAndEndTimestampIsExclusiveInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
            // some delay
            for(int j = 0; j < 10000000; j++);
        }
        // get exact timestamps at initial add and final add
        Map<Timestamp, Set<Object>> chronologie = client.
                chronologize(key, record);
        Timestamp exactStartTimestamp = Variables.register("exactStartTimestamp", 
                Iterables.getFirst((Iterable<Timestamp>) chronologie.keySet(), null));
        Timestamp exactEndTimestamp = Variables.register("exactEndTimestamp",
                Iterables.getLast((Iterable<Timestamp>) chronologie.keySet()));
        
        // end timestamp is the exactStartTimestamp
        Map<Timestamp, Set<Object>> result = client.
                chronologize(key, record, Timestamp.epoch(), exactStartTimestamp);               
        Set<Object> lastResultSet = null;
        assertTrue(result.isEmpty()); 

        // end timestamp is after the exactStartTimestamp
        result = client.chronologize(key, record,
                Timestamp.epoch(), timestamps.get(0));
        lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(1, lastResultSet.size());
        
        // end timestamp is after the exactStartTimestamp
        // and start timestamp is exactStartTimestamp
        result = client.chronologize(key, record,
                exactStartTimestamp, timestamps.get(0));
        lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(1, lastResultSet.size());
        
        // start timestamp is exactEndTimestamp
        result = client.chronologize(key, record,
                exactEndTimestamp, Timestamp.now());
        lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(testSize, lastResultSet.size());
        
        // start timestamp is after exactEndTimestamp
        result = client.chronologize(key, record,
                timestamps.get(testSize-1), Timestamp.now());
        lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(testSize, lastResultSet.size());
    }
    
    public void testChronologizeWithStartTimestampEqualsEndTimestampInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
            // some delay
            for (int j = 0; j < 10000000; j++);
        }
        // check same timestamps before initial add
        Map<Timestamp, Set<Object>> result = client.chronologize(
                key, record, Timestamp.epoch(), Timestamp.epoch());
        Set<Object> lastResultSet = null;
        assertTrue(result.isEmpty());
        
        // check same timstamps at exact initial add
        Map<Timestamp, Set<Object>> chronologie = client.chronologize(
                key, record);
        Timestamp exactStartTimestamp = Iterables.getFirst(
                chronologie.keySet(), null);
        result = client.chronologize(key, record, 
                exactStartTimestamp, exactStartTimestamp);
        lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(1, lastResultSet.size());
        
        // check same timestamps after all the adds
        result = client.chronologize(key, record,
                Timestamp.now(), Timestamp.now());
        lastResultSet = Iterables.getLast(result.values());
        assertEquals(1, result.size());
        assertEquals(testSize, lastResultSet.size());
    }
    
    @Test
    public void testChronologizeWithStartTimestampGreaterThanEndTimestampInKeyInRecord() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
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
            for (int j = 0; j < 10000000; j++);  // some delay
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(
                key, record, timestamps.get(3), timestamps.get(2));
        assertTrue(result.isEmpty());
    }
}
