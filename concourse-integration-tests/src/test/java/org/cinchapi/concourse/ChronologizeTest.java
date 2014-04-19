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
}
