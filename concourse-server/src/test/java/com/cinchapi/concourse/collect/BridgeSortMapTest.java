/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.collect;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link BridgeSortMap}.
 *
 * @author Jeff Nelson
 */
public class BridgeSortMapTest {

    @Test
    public void testMaintainsSortedOrderDuringIteration() {
        // Setup a provided map with some entries
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("apple", 2);
        provided.put("banana", 1);
        provided.put("grape", 3);

        // Create the BridgeSortMap with a natural order comparator
        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        // Add new entries in random order
        map.put("date", 5);
        map.put("cherry", 4);
        map.put("elderberry", 6);

        // Collect keys during iteration to verify order
        List<String> iteratedKeys = new ArrayList<>();
        for (Entry<String, Integer> entry : map.entrySet()) {
            iteratedKeys.add(entry.getKey());
        }

        // Expected keys in sorted order
        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("apple");
        expectedKeys.add("banana");
        expectedKeys.add("cherry");
        expectedKeys.add("date");
        expectedKeys.add("elderberry");
        expectedKeys.add("grape");

        Assert.assertEquals("Keys should be in sorted order during iteration",
                expectedKeys, iteratedKeys);
    }

    @Test
    public void testBridgesSortingBetweenProvidedAndIncrementalMaps() {
        // Setup a provided map with entries already in sorted order
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("apple", 1);
        provided.put("banana", 2);
        provided.put("grape", 5);

        // Create the BridgeSortMap with a natural order comparator
        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        // Add new entries that should be interleaved with existing entries
        map.put("cherry", 3); // Should appear between banana and grape
        map.put("date", 4); // Should appear between cherry and grape

        // Verify the iteration order is correct
        Iterator<String> it = map.keySet().iterator();
        Assert.assertEquals("apple", it.next());
        Assert.assertEquals("banana", it.next());
        Assert.assertEquals("cherry", it.next());
        Assert.assertEquals("date", it.next());
        Assert.assertEquals("grape", it.next());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testCorrectlyUpdatesExistingKeys() {
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("apple", 1);
        provided.put("banana", 2);

        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        // Add a new entry to the incremental map
        map.put("cherry", 3);

        // Update existing keys in both maps
        map.put("apple", 10); // Update in provided map
        map.put("cherry", 30); // Update in incremental map

        // Verify updates were applied correctly
        Assert.assertEquals(Integer.valueOf(10), map.get("apple"));
        Assert.assertEquals(Integer.valueOf(10), provided.get("apple"));
        Assert.assertEquals(Integer.valueOf(2), map.get("banana"));
        Assert.assertEquals(Integer.valueOf(30), map.get("cherry"));

        // Verify the iteration order is still correct
        Iterator<Entry<String, Integer>> it = map.entrySet().iterator();
        Entry<String, Integer> entry = it.next();
        Assert.assertEquals("apple", entry.getKey());
        Assert.assertEquals(Integer.valueOf(10), entry.getValue());

        entry = it.next();
        Assert.assertEquals("banana", entry.getKey());
        Assert.assertEquals(Integer.valueOf(2), entry.getValue());

        entry = it.next();
        Assert.assertEquals("cherry", entry.getKey());
        Assert.assertEquals(Integer.valueOf(30), entry.getValue());
    }

    @Test
    public void testRemovalMaintainsSortedOrder() {
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("apple", 1);
        provided.put("banana", 2);
        provided.put("grape", 5);

        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        // Add new entries to the incremental map
        map.put("cherry", 3);
        map.put("date", 4);

        // Remove entries from both maps
        map.remove("banana"); // Remove from provided map
        map.remove("date"); // Remove from incremental map

        // Verify the entries were removed
        Assert.assertNull(provided.get("banana"));
        Assert.assertNull(map.get("banana"));
        Assert.assertNull(map.get("date"));

        // Verify the iteration order is still correct
        List<String> keys = new ArrayList<>();
        for (String key : map.keySet()) {
            keys.add(key);
        }

        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("apple");
        expectedKeys.add("cherry");
        expectedKeys.add("grape");

        Assert.assertEquals("Keys should remain in sorted order after removal",
                expectedKeys, keys);
    }

    @Test
    public void testCustomComparatorSorting() {
        // Create a custom comparator that sorts strings by length, then
        // alphabetically
        Comparator<String> lengthThenAlpha = Comparator
                .comparing(String::length)
                .thenComparing(Comparator.naturalOrder());

        // Create a provided map with entries already in sorted order according
        // to the comparator
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("fig", 3); // 3 chars
        provided.put("kiwi", 5); // 4 chars
        provided.put("pear", 4); // 4 chars
        provided.put("apple", 1); // 5 chars
        provided.put("grape", 6); // 5 chars
        provided.put("banana", 2); // 6 chars

        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                lengthThenAlpha);

        // Add new entries
        map.put("plum", 7); // 4 chars
        map.put("mango", 8); // 5 chars - should be between apple and grape

        // Expected order: fig (3), kiwi (4), plum (4), pear (4), apple (5),
        // mango (5), grape (5), banana (6)
        List<String> expectedOrder = new ArrayList<>();
        expectedOrder.add("fig");
        expectedOrder.add("kiwi");
        expectedOrder.add("pear");
        expectedOrder.add("plum");
        expectedOrder.add("apple");
        expectedOrder.add("grape");
        expectedOrder.add("mango");
        expectedOrder.add("banana");

        // Verify the iteration order follows the custom comparator
        List<String> actualOrder = new ArrayList<>();
        for (String key : map.keySet()) {
            actualOrder.add(key);
        }

        Assert.assertEquals("Keys should be sorted by custom comparator",
                expectedOrder, actualOrder);
    }

    @Test
    public void testPutAllMaintainsSortedOrder() {
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("apple", 1);
        provided.put("grape", 5);

        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        // Create a batch of new entries
        Map<String, Integer> batch = new HashMap<>();
        batch.put("banana", 2);
        batch.put("cherry", 3);
        batch.put("date", 4);
        batch.put("apple", 10); // Should update existing entry

        // Add all entries at once
        map.putAll(batch);

        // Verify updates were applied correctly
        Assert.assertEquals(Integer.valueOf(10), map.get("apple"));
        Assert.assertEquals(Integer.valueOf(10), provided.get("apple"));
        Assert.assertEquals(Integer.valueOf(2), map.get("banana"));
        Assert.assertEquals(Integer.valueOf(3), map.get("cherry"));
        Assert.assertEquals(Integer.valueOf(4), map.get("date"));
        Assert.assertEquals(Integer.valueOf(5), map.get("grape"));

        // Verify the iteration order is correct
        List<String> keys = new ArrayList<>();
        for (String key : map.keySet()) {
            keys.add(key);
        }

        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("apple");
        expectedKeys.add("banana");
        expectedKeys.add("cherry");
        expectedKeys.add("date");
        expectedKeys.add("grape");

        Assert.assertEquals("Keys should be in sorted order after putAll",
                expectedKeys, keys);
    }

    @Test
    public void testValuesFollowKeyOrder() {
        Map<String, Integer> provided = new LinkedHashMap<>();
        provided.put("apple", 1);
        provided.put("banana", 2);

        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        map.put("cherry", 3);
        map.put("date", 4);

        // Values should be in the same order as the sorted keys
        List<Integer> values = new ArrayList<>();
        for (Integer value : map.values()) {
            values.add(value);
        }

        List<Integer> expectedValues = new ArrayList<>();
        expectedValues.add(1); // apple
        expectedValues.add(2); // banana
        expectedValues.add(3); // cherry
        expectedValues.add(4); // date

        Assert.assertEquals("Values should follow the sorted key order",
                expectedValues, values);
    }

    @Test
    public void testWithSortedMapAsProvided() {
        // Create a SortedMap with natural order
        SortedMap<String, Integer> provided = new TreeMap<>(
                Comparator.naturalOrder());
        provided.put("apple", 1);
        provided.put("banana", 2);
        provided.put("grape", 3);

        // Create BridgeSortMap with the same natural order
        BridgeSortMap<String, Integer> map = new BridgeSortMap<>(provided,
                Comparator.naturalOrder());

        map.put("cherry", 4);
        map.put("date", 5);

        // Verify the iteration order is correct
        List<String> keys = new ArrayList<>();
        for (String key : map.keySet()) {
            keys.add(key);
        }

        List<String> expectedKeys = new ArrayList<>();
        expectedKeys.add("apple");
        expectedKeys.add("banana");
        expectedKeys.add("cherry");
        expectedKeys.add("date");
        expectedKeys.add("grape");

        Assert.assertEquals("Keys should be in natural order", expectedKeys,
                keys);
    }

    @Test
    public void testEqualsAndHashCode() {
        // Create two maps with the same content
        Map<String, Integer> provided1 = new LinkedHashMap<>();
        provided1.put("apple", 1);
        provided1.put("banana", 2);

        BridgeSortMap<String, Integer> map1 = new BridgeSortMap<>(provided1,
                Comparator.naturalOrder());
        map1.put("cherry", 3);

        Map<String, Integer> provided2 = new LinkedHashMap<>();
        provided2.put("apple", 1);
        provided2.put("banana", 2);

        BridgeSortMap<String, Integer> map2 = new BridgeSortMap<>(provided2,
                Comparator.naturalOrder());
        map2.put("cherry", 3);

        // They should be equal and have the same hashCode
        Assert.assertEquals(map1, map2);
        Assert.assertEquals(map1.hashCode(), map2.hashCode());

        // Create a regular map with the same entries
        Map<String, Integer> regularMap = new HashMap<>();
        regularMap.put("apple", 1);
        regularMap.put("banana", 2);
        regularMap.put("cherry", 3);

        // The BridgeSortMap should equal the regular map
        Assert.assertEquals(map1, regularMap);

        // Modify one map
        map2.put("date", 4);

        // They should no longer be equal
        Assert.assertFalse(map1.equals(map2));
    }

}
