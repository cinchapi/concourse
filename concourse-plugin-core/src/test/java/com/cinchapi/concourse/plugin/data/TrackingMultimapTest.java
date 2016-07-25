package com.cinchapi.concourse.plugin.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.plugin.data.TrackingMultimap;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for {@link TrackingMultimap}.
 * Confirms that delegation of methods to an internal {@code Map} functions as desired.
 * 
 * @author Aditya Srinivasan
 *
 */
@Ignore
public class TrackingMultimapTest extends ConcourseBaseTest {
    
    private Map<String, Set<Integer>> map;
    
    @Override
    public void beforeEachTest() {
        map = TrackingLinkedHashMultimap.create();
    }

    @Override
    public void afterEachTest() {
        map = null;
    }
    
    @Test
    public void testSize() {
        int count = Random.getScaleCount();
        for(int i = 0; i < count; i++) {
            map.put(Random.getString(), new HashSet<Integer>());
        }
        Assert.assertTrue(map.size() == count);
    }
    
    @Test
    public void testIsEmpty() {
        Assert.assertTrue(map.isEmpty());
        for(int i = 0; i < Random.getScaleCount(); i++) {
            map.put(Random.getString(), new HashSet<Integer>());
        }
        Assert.assertFalse(map.isEmpty());
    }
    
    @Test
    public void testContainsKey() {
        String key = Random.getString();
        Assert.assertFalse(map.containsKey(key));
        map.put(key, new HashSet<Integer>());
        Assert.assertTrue(map.containsKey(key));
    }
    
    @Test
    public void testContainsValue() {
        Set<Integer> value = new HashSet<Integer>();
        Assert.assertFalse(map.containsValue(value));
        map.put(Random.getString(), value);
        Assert.assertTrue(map.containsValue(value));
    }
    
    @Test
    public void testGet() {
        String key = Random.getString();
        Set<Integer> value = new HashSet<Integer>();
        map.put(key, value);
        Assert.assertEquals(map.get(key), value);
        Assert.assertFalse(map.get(key) == new HashSet<Integer>());
    }
    
    @Test
    public void testPut() {
        String key = Random.getString();
        Set<Integer> value1 = new HashSet<Integer>();
        Set<Integer> value2 = new HashSet<Integer>();
        Assert.assertTrue(map.get(key) == null);
        Assert.assertTrue(map.put(key, value1) == null);
        Assert.assertTrue(map.put(key, value2) == value1);
    }
    
    @Test
    public void testRemove() {
        String key = Random.getString();
        Set<Integer> value1 = new HashSet<Integer>();
        Set<Integer> value2 = new HashSet<Integer>();
        Assert.assertTrue(map.remove(key) == null);
        map.put(key, value1);
        Assert.assertTrue(map.remove(key) == value1);
        map.put(key, value2);
        Assert.assertTrue(map.remove(key) == value2);
    }
    
    @Test
    public void testClear() {
        int count = Random.getScaleCount();
        for(int i = 0; i < count; i++) {
            map.put(Random.getString(), new HashSet<Integer>());
        }
        Assert.assertFalse(map.isEmpty());
        map.clear();
        Assert.assertTrue(map.isEmpty());
    }
    
    @Test
    public void testPutAll() {
        Map<String, Set<Integer>> map1 = new HashMap<String, Set<Integer>>();
        int count1 = Random.getScaleCount();
        for(int i = 0; i < count1; i++) {
            map1.put(Random.getString(), new HashSet<Integer>());
        }
        int count2 = Random.getScaleCount();
        for(int i = 0; i < count2; i++) {
            map.put(Random.getString(), new HashSet<Integer>());
        }
        Assert.assertTrue(map.size() == count2);
        map.putAll(map1);
        Assert.assertTrue(map.size() == (count1 + count2));
    }
    
    @Test
    public void testKeySet() {
        Set<String> keys = new HashSet<String>();
        for(int i = 0; i < Random.getScaleCount(); i++) {
            String key = Random.getSimpleString();
            keys.add(key);
            map.put(key, new HashSet<Integer>());
        }
        for(String key : map.keySet()) {
            Assert.assertTrue(keys.contains(key));
        }
        Assert.assertTrue(keys.size() == map.keySet().size());
    }
    
    @Test
    public void testValues() {
        List<Set<Integer>> values = new ArrayList<Set<Integer>>();
        for(int i = 0; i < Random.getScaleCount(); i++) {
            Set<Integer> value = new HashSet<Integer>();
            values.add(value);
            map.put(Random.getString(), value);
        }
        for(Set<Integer> value : map.values()) {
            Assert.assertTrue(values.contains(value));
        }
        Assert.assertTrue(values.size() == map.values().size());
    }
    
    @Test
    public void testEntrySet() {
        List<String> keys = new ArrayList<String>();
        List<Set<Integer>> values = new ArrayList<Set<Integer>>();
        for(int i = 0; i < Random.getScaleCount(); i++) {
            String key = Random.getString();
            Set<Integer> value = new HashSet<Integer>();
            keys.add(key);
            values.add(value);
            map.put(key, value);
        }
        for(Entry<String, Set<Integer>> entry : map.entrySet()) {
            Assert.assertTrue(keys.contains(entry.getKey()));
            Assert.assertTrue(values.contains(entry.getValue()));
        }
        Assert.assertTrue(keys.size() == map.entrySet().size());
        Assert.assertTrue(values.size() == map.entrySet().size());
    }

}
