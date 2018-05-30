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
package com.cinchapi.concourse.server.plugin.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.server.plugin.data.TrackingMultimap.DataType;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link TrackingMultimap}. Confirms that delegation of methods
 * to an internal {@link Map} functions as desired.
 * 
 * @author Aditya Srinivasan
 *
 */
public class TrackingMultimapTest
        extends TrackingMultimapBaseTest<String, Integer> {

    @Test
    public void testSize() {
        int count = Random.getScaleCount();
        for (int i = 0; i < count; i++) {
            boolean added = false;
            while (!added) {
                added = map.put(Random.getString(), randomValueSet()) == null;
            }
        }
        Assert.assertEquals(map.size(), count);
    }

    @Test
    public void testIsEmpty() {
        Assert.assertTrue(map.isEmpty());
        for (int i = 0; i < Random.getScaleCount(); i++) {
            map.put(Random.getString(), randomValueSet());
        }
        Assert.assertFalse(map.isEmpty());
    }

    @Test
    public void testContainsKey() {
        String key = Random.getString();
        Assert.assertFalse(map.containsKey(key));
        map.put(key, randomValueSet());
        Assert.assertTrue(map.containsKey(key));
    }

    @Test
    public void testContainsValue() {
        Set<Integer> value = randomValueSet();
        Assert.assertFalse(map.containsValue(value));
        map.put(Random.getString(), value);
        Assert.assertTrue(map.containsValue(value));
    }

    @Test
    public void testGet() {
        String key = Random.getString();
        Set<Integer> value = randomValueSet();
        map.put(key, value);
        Assert.assertEquals(map.get(key), value);
        Assert.assertFalse(map.get(key) == randomValueSet());
    }

    @Test
    public void testPut() {
        String key = Random.getString();
        Set<Integer> value1 = randomValueSet();
        Set<Integer> value2 = randomValueSet();
        Assert.assertNull(map.get(key));
        Assert.assertNull(map.put(key, value1));
        Assert.assertEquals(map.put(key, value2), value1);
    }

    @Test
    public void testPutEmptySet() {
        String key = Random.getString();
        map.put(key, Sets.newHashSet());
        Assert.assertNotNull(map.get(key));
    }

    @Test
    public void testRemove() {
        String key = Random.getString();
        Set<Integer> value1 = randomValueSet();
        Set<Integer> value2 = randomValueSet();
        Assert.assertNull(map.remove(key));
        map.put(key, value1);
        Assert.assertEquals(map.remove(key), value1);
        map.put(key, value2);
        Assert.assertEquals(map.remove(key), value2);
    }

    @Test
    public void testClear() {
        int count = Random.getScaleCount();
        for (int i = 0; i < count; i++) {
            map.put(Random.getString(), randomValueSet());
        }
        Assert.assertFalse(map.isEmpty());
        map.clear();
        Assert.assertTrue(map.isEmpty());
    }

    @Test
    public void testPutAll() {
        Map<String, Set<Integer>> map1 = new HashMap<String, Set<Integer>>();
        int count1 = Random.getScaleCount();
        for (int i = 0; i < count1; i++) {
            map1.put(Random.getString(), randomValueSet());
        }
        int count2 = Random.getScaleCount();
        for (int i = 0; i < count2; i++) {
            map.put(Random.getString(), randomValueSet());
        }
        Assert.assertTrue(map.size() == count2);
        map.putAll(map1);
        Variables.register("map", map.keySet());
        Variables.register("map1", map1.keySet());
        Assert.assertTrue(map.size() == (count1 + count2));
    }

    @Test
    public void testKeySet() {
        Set<String> keys = new HashSet<String>();
        for (int i = 0; i < Random.getScaleCount(); i++) {
            String key = Random.getSimpleString();
            keys.add(key);
            map.put(key, randomValueSet());
        }
        for (String key : map.keySet()) {
            Assert.assertTrue(keys.contains(key));
        }
        Assert.assertTrue(keys.size() == map.keySet().size());
    }

    @Test
    public void testValues() {
        List<Set<Integer>> values = new ArrayList<Set<Integer>>();
        for (int i = 0; i < Random.getScaleCount(); i++) {
            Set<Integer> value = randomValueSet();
            values.add(value);
            map.put(Random.getString(), value);
        }
        for (Set<Integer> value : map.values()) {
            Assert.assertTrue(values.contains(value));
        }
        Assert.assertTrue(values.size() == map.values().size());
    }

    @Test
    public void testEntrySet() {
        List<String> keys = new ArrayList<String>();
        List<Set<Integer>> values = new ArrayList<Set<Integer>>();
        for (int i = 0; i < Random.getScaleCount(); i++) {
            String key = Random.getString();
            Set<Integer> value = randomValueSet();
            keys.add(key);
            values.add(value);
            map.put(key, value);
        }
        for (Entry<String, Set<Integer>> entry : map.entrySet()) {
            Assert.assertTrue(keys.contains(entry.getKey()));
            Assert.assertTrue(values.contains(entry.getValue()));
        }
        Assert.assertTrue(keys.size() == map.entrySet().size());
        Assert.assertTrue(values.size() == map.entrySet().size());
    }

    @Test
    public void testDistinctivenessNoDupes() {
        TrackingMultimap<String, Integer> tmmap = (TrackingMultimap<String, Integer>) map;
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            map.put(Random.getString(), Sets.newHashSet(Random.getInt()));
        }
        Assert.assertEquals(1, tmmap.distinctiveness(), 0);
    }

    @Test
    public void testDistinctivenessSomeDupes() {
        TrackingMultimap<String, Integer> tmmap = (TrackingMultimap<String, Integer>) map;
        tmmap.put("a", Sets.newHashSet(1, 2, 3));
        tmmap.put("b", Sets.newHashSet(1, 2, 3));
        tmmap.put("c", Sets.newHashSet(1, 2, 3, 4));
        Assert.assertEquals(0.3, tmmap.distinctiveness(), 0);
    }

    @Test
    public void testCount() {
        TrackingMultimap<String, Integer> tmmap = (TrackingMultimap<String, Integer>) map;
        tmmap.put("a", Sets.newHashSet(1, 2, 3));
        tmmap.put("b", Sets.newHashSet(1, 2, 3));
        tmmap.put("c", Sets.newHashSet(1, 2, 3, 4));
        AtomicInteger count = new AtomicInteger(0);
        tmmap.forEach((key, values) -> {
            count.addAndGet(values.size());
        });
        Assert.assertEquals(tmmap.count(), count.get());
    }

    @Test
    public void testDeleteWillRemoveKeyIfNoAssociatedValues() {
        TrackingMultimap<String, Integer> tmmap = (TrackingMultimap<String, Integer>) map;
        tmmap.insert("a", 1);
        tmmap.insert("a", 2);
        tmmap.delete("a", 1);
        tmmap.delete("a", 2);
        Assert.assertFalse(tmmap.containsKey("a"));
    }

    @Test
    public void testMin() {
        TrackingMultimap<Object, Long> tmmap = TrackingLinkedHashMultimap
                .create(ObjectResultDataset.OBJECT_COMPARATOR);
        tmmap.insert(2, 1L);
        tmmap.insert(1, 2L);
        tmmap.insert(30, 3L);
        tmmap.insert(4, 4L);
        Assert.assertEquals(1, tmmap.min());
    }

    @Test
    public void testMinAfterRemove() {
        TrackingMultimap<Object, Long> tmmap = TrackingLinkedHashMultimap
                .create(ObjectResultDataset.OBJECT_COMPARATOR);
        tmmap.insert(2, 1L);
        tmmap.insert(1, 2L);
        tmmap.insert(1, 20L);
        tmmap.insert(30, 3L);
        tmmap.insert(4, 4L);
        tmmap.delete(1, 2L);
        Assert.assertEquals(1, tmmap.min());
        tmmap.delete(1, 20L);
        Assert.assertEquals(2, tmmap.min());
    }

    @Test
    public void testMax() {
        TrackingMultimap<Object, Long> tmmap = TrackingLinkedHashMultimap
                .create(ObjectResultDataset.OBJECT_COMPARATOR);
        tmmap.insert(2, 1L);
        tmmap.insert(1, 2L);
        tmmap.insert(30, 3L);
        tmmap.insert(4, 4L);
        Assert.assertEquals(30, tmmap.max());
    }

    @Test
    public void testMaxAfterRemove() {
        TrackingMultimap<Object, Long> tmmap = TrackingLinkedHashMultimap
                .create(ObjectResultDataset.OBJECT_COMPARATOR);
        tmmap.insert(2, 1L);
        tmmap.insert(1, 2L);
        tmmap.insert(1, 20L);
        tmmap.insert(30, 3L);
        tmmap.insert(30, 30L);
        tmmap.insert(4, 4L);
        tmmap.delete(30, 3L);
        Assert.assertEquals(30, tmmap.max());
        tmmap.delete(30, 30L);
        Assert.assertEquals(4, tmmap.max());
    }

    @Test
    public void testPercentKeyDataTypeWhenEmpty() {
        TrackingMultimap<Object, Long> tmmap = TrackingLinkedHashMultimap
                .create(ObjectResultDataset.OBJECT_COMPARATOR);
        Assert.assertEquals(0, tmmap.percentKeyDataType(DataType.NUMBER), 0);
    }

    @Test
    public void testPercentKeyDataTypeTimestamp() {
        TrackingMultimap<Object, Long> tmmap = TrackingLinkedHashMultimap
                .create(ObjectResultDataset.OBJECT_COMPARATOR);
        Timestamp t1 = Timestamp.now();
        Timestamp t2 = Timestamp.now();
        Timestamp t3 = Timestamp.now();
        Object t4 = Random.getObject();
        tmmap.insert(t1, 1L);
        tmmap.insert(t2, 2L);
        tmmap.insert(t3, 1L);
        tmmap.insert(t4, 17L);
        Assert.assertEquals(0.75, tmmap.percentKeyDataType(DataType.TIMESTAMP),
                0);
    }

    /**
     * Return a random {@link Set} to use within tests.
     * 
     * @return the random set
     */
    private static Set<Integer> randomValueSet() {
        int count = Random.getScaleCount();
        Set<Integer> set = Sets.newHashSetWithExpectedSize(count);
        for (int i = 0; i < count; ++i) {
            set.add(Random.getInt());
        }
        return set;
    }

}
