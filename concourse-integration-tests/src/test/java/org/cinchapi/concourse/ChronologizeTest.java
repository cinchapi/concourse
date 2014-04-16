package org.cinchapi.concourse;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Test;

import com.google.common.collect.Sets;

public class ChronologizeTest extends ConcourseIntegrationTest {

    @Test
    public void testWithoutRemoval() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
        int testSize = Variables.register("testSize", 5);
        Set<Object> initValues = Variables.register("initValues", 
                Sets.newHashSet());
        // test no values
        assertTrue(client.chronologize(key, record).isEmpty());
        for (int i = 0; i < testSize; i++) {
            Object value = null;
            while (value == null || initValues.contains(value)) {
                value = TestData.getObject();
            }
            initValues.add(value);
            boolean isAdded = client.add(key, value, record);
            // test if the value has been added
            assertTrue(isAdded);
        }
        Map<Timestamp, Set<Object>> result = client.chronologize(key, record);
        // test size of returned map
        assertEquals(testSize, result.size());
        Iterator<Map.Entry<Timestamp, Set<Object>>> it = result.entrySet().iterator();
        for (int i = 0; i < testSize; i++) {
            // test size of each set of values
            assertEquals(i+1, it.next().getValue().size());
        }
    }
    
    @Test
    public void testWithRemoval() {
        long record = Variables.register("record", client.create());
        String key = Variables.register("key", TestData.getString());
        Map<Timestamp, Set<Object>> result;
        Object v0 = TestData.getObject();
        Object v1 = TestData.getObject();
        Object v2 = TestData.getObject();
        Object v3 = TestData.getObject();
        Object v4 = TestData.getObject();
        
        // initially
        assertTrue(client.chronologize(key, record).isEmpty());
        
        client.add(key, v0, record);
        client.add(key, v1, record);
        client.add(key, v2, record);
        client.add(key, v3, record);
        client.add(key, v4, record);
        
        // after 5 consecutive adds
        // test sizes of a few sets
        result = client.chronologize(key, record);
        assertEquals(5, result.size());
        assertEquals(1, getSetInMap(result, 0).size());
        assertEquals(3, getSetInMap(result, 2).size());
        assertEquals(5, getSetInMap(result, 4).size());
        
        // remove 3 values consecutively
        // test size of final set in map after each remove
        client.remove(key, v2, record);
        result = client.chronologize(key, record);
        assertFalse(client.verify(key, v2, record));
        assertEquals(6, result.size());
        assertEquals(4, getSetInMap(result, 5).size());
        
        client.remove(key, v0, record);
        result = client.chronologize(key, record);
        assertFalse(client.verify(key, v0, record));
        assertEquals(7, result.size());
        assertEquals(3, getSetInMap(result, 6).size());
        
        client.remove(key, v4, record);
        result = client.chronologize(key, record);
        assertFalse(client.verify(key, v4, record));
        assertEquals(8, result.size());
        assertEquals(2, getSetInMap(result, 7).size());
        
        // add one value again
        client.add(key, v2, record);
        result = client.chronologize(key, record);
        assertTrue(client.verify(key, v2, record));
        assertEquals(9, result.size());
        assertEquals(3, getSetInMap(result, 8).size());
        
        // add v2 again but should fail
        assertFalse(client.add(key, v2, record));
        result = client.chronologize(key, record);
        assertEquals(9, result.size());
        assertEquals(3, getSetInMap(result, 8).size());
        
        // remove value not in key in record
        Object v5 = TestData.getObject();
        while (v5.equals(v0) || v5.equals(v1) ||
                v5.equals(v2) || v5.equals(v3) || v5.equals(v4)) {
            v5 = TestData.getObject();
        }
        client.remove(key, v5, record);
        result = client.chronologize(key, record);
        assertEquals(9, result.size());
        assertEquals(3, getSetInMap(result, 8).size());
        
        // clear all, empty set is filtered and not include in returned map
        // right now, 3 values left, so should add 2 more timestamp
        client.clear(key, record);
        result = client.chronologize(key, record);
        assertEquals(11, result.size());
        assertEquals(1, getSetInMap(result, 10).size());
        
        // set some values again
        client.set(key, v0, record);
        result = client.chronologize(key, record);
        assertEquals(12, result.size());
        assertEquals(1, getSetInMap(result, 11).size());
        assertTrue(getSetInMap(result, 11).contains(v0));
        
        client.set(key, v1, record);
        result = client.chronologize(key, record);
        assertEquals(13, result.size());
        assertEquals(1, getSetInMap(result, 12).size());
        assertTrue(getSetInMap(result, 12).contains(v1));
        assertTrue(getSetInMap(result, 11).contains(v0));
       
    }
    
    /**
     * 
     * @param map
     * @param index
     * @return
     */
    private static Set<Object> getSetInMap(Map<Timestamp, Set<Object>> map, int index) {
        if (index < 0 || index >= map.size()) {
            return null;
        }
        Iterator<Map.Entry<Timestamp, Set<Object>>> it = map.entrySet().iterator();
        int i = 0;
        Set<Object> finalSet = null;
        while (it.hasNext()) {
            finalSet = it.next().getValue();
            if (index == i) {
                break;
            }
            i++;
        }
        return finalSet;
    }
    

}
