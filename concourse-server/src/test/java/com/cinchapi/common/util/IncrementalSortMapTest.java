package com.cinchapi.common.util;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.common.util.IncrementalSortMap;
import com.cinchapi.concourse.test.ConcourseBaseTest;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class IncrementalSortMapTest extends ConcourseBaseTest {

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private static IncrementalSortMap<Integer, String> map5() {
        IncrementalSortMap<Integer, String> map = IncrementalSortMap.create();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(five, "E");
        map.put(three, "C");
        map.put(two, "B");
        map.put(four, "D");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    static void assertEq(Integer i, int j) {
        if(i == null)
            assertEquals(j, -1);
        else
            assertEquals((int) i, j);
    }

    static boolean eq(Integer i, int j) {
        return i == null ? j == -1 : i == j;
    }

    static NavigableMap<Integer, Integer> newMap(Class cl) throws Exception {
        NavigableMap<Integer, Integer> result = (NavigableMap<Integer, Integer>) cl
                .newInstance();
        assertEquals(0, result.size());
        assertFalse(result.keySet().iterator().hasNext());
        return result;
    }

    private static final Integer five = 5;
    private static final Integer four = 4;

    private static final Integer one = 1;

    private static final Integer six = 6;

    private static final Integer three = 3;

    private static final Integer two = 2;

    private static final Integer zero = 0;

    BitSet bs;

    boolean expensiveTests = true;

    Random rnd = new Random(666);

    /**
     * ceilingEntry returns next entry.
     */
    @Test
    public void testCeilingEntry() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e1 = map.ceilingEntry(three);
        assertEquals(three, e1.getKey());

        Map.Entry e2 = map.ceilingEntry(zero);
        assertEquals(one, e2.getKey());

        Map.Entry e3 = map.ceilingEntry(five);
        assertEquals(five, e3.getKey());

        Map.Entry e4 = map.ceilingEntry(six);
        assertNull(e4);
    }

    /**
     * ceilingKey returns next element
     */
    @Test
    public void testCeilingKey() {
        IncrementalSortMap<Integer, String> q = map5();
        Object e1 = q.ceilingKey(three);
        assertEquals(three, e1);

        Object e2 = q.ceilingKey(zero);
        assertEquals(one, e2);

        Object e3 = q.ceilingKey(five);
        assertEquals(five, e3);

        Object e4 = q.ceilingKey(six);
        assertNull(e4);
    }

    /**
     * clear removes all pairs
     */
    @Test
    public void testClear() {
        IncrementalSortMap<Integer, String> map = map5();
        map.clear();
        assertEquals(0, map.size());
    }

    /**
     * containsKey returns true for contained key
     */
    @Test
    public void testContainsKey() {
        IncrementalSortMap<Integer, String> map = map5();
        assertTrue(map.containsKey(one));
        assertFalse(map.containsKey(zero));
    }

    /**
     * containsKey(null) of nonempty map throws NPE
     */
    @Test
    public void testContainsKey_NullPointerException() {
        try {
            IncrementalSortMap<Integer, String> c = map5();
            c.containsKey(null);
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * containsValue returns true for held values
     */
    @Test
    public void testContainsValue() {
        IncrementalSortMap<Integer, String> map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }

    /**
     * containsValue(null) throws NPE
     */
    @Test
    public void testContainsValue_NullPointerException() {
        try {
            IncrementalSortMap<Integer, String> c = IncrementalSortMap.create();
            c.containsValue(null);
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * descendingEntrySet contains all pairs
     */
    @Test
    public void testDescendingEntrySet() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.descendingMap().entrySet();
        assertEquals(5, s.size());
        Iterator it = s.iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            assertTrue((e.getKey().equals(one) && e.getValue().equals("A"))
                    || (e.getKey().equals(two) && e.getValue().equals("B"))
                    || (e.getKey().equals(three) && e.getValue().equals("C"))
                    || (e.getKey().equals(four) && e.getValue().equals("D"))
                    || (e.getKey().equals(five) && e.getValue().equals("E")));
        }
    }

    /**
     * descendingEntrySet.toArray contains all entries
     */
    @Test
    public void testDescendingEntrySetToArray() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.descendingMap().entrySet();
        Object[] ar = s.toArray();
        assertEquals(5, ar.length);
        for (int i = 0; i < 5; ++i) {
            assertTrue(map.containsKey(((Map.Entry) (ar[i])).getKey()));
            assertTrue(map.containsValue(((Map.Entry) (ar[i])).getValue()));
        }
    }

    /**
     * descending iterator of descendingKeySet is ordered
     */
    @Test
    public void testDescendingKeySetDescendingIteratorOrder() {
        IncrementalSortMap<Integer, String> map = map5();
        NavigableSet s = map.descendingKeySet();
        Iterator i = s.descendingIterator();
        Integer last = (Integer) i.next();
        assertEquals(last, one);
        int count = 1;
        while (i.hasNext()) {
            Integer k = (Integer) i.next();
            assertTrue(last.compareTo(k) < 0);
            last = k;
            ++count;
        }
        assertEquals(5, count);
    }

    /**
     * descendingKeySet is ordered
     */
    @Test
    public void testDescendingKeySetOrder() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.descendingKeySet();
        Iterator i = s.iterator();
        Integer last = (Integer) i.next();
        assertEquals(last, five);
        int count = 1;
        while (i.hasNext()) {
            Integer k = (Integer) i.next();
            assertTrue(last.compareTo(k) > 0);
            last = k;
            ++count;
        }
        assertEquals(5, count);
    }

    /**
     * lowerEntry, higherEntry, ceilingEntry, and floorEntry return
     * immutable entries
     */
    @Test
    public void testEntryImmutability() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e = map.lowerEntry(three);
        assertEquals(two, e.getKey());
        try {
            e.setValue("X");
            shouldThrow();
        }
        catch (UnsupportedOperationException success) {}
        e = map.higherEntry(zero);
        assertEquals(one, e.getKey());
        try {
            e.setValue("X");
            shouldThrow();
        }
        catch (UnsupportedOperationException success) {}
        e = map.floorEntry(one);
        assertEquals(one, e.getKey());
        try {
            e.setValue("X");
            shouldThrow();
        }
        catch (UnsupportedOperationException success) {}
        e = map.ceilingEntry(five);
        assertEquals(five, e.getKey());
        try {
            e.setValue("X");
            shouldThrow();
        }
        catch (UnsupportedOperationException success) {}
    }

    /**
     * entrySet contains all pairs
     */
    @Test
    public void testEntrySet() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.entrySet();
        assertEquals(5, s.size());
        Iterator it = s.iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            assertTrue((e.getKey().equals(one) && e.getValue().equals("A"))
                    || (e.getKey().equals(two) && e.getValue().equals("B"))
                    || (e.getKey().equals(three) && e.getValue().equals("C"))
                    || (e.getKey().equals(four) && e.getValue().equals("D"))
                    || (e.getKey().equals(five) && e.getValue().equals("E")));
        }
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Test
    public void testEntrySetToArray() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.entrySet();
        Object[] ar = s.toArray();
        assertEquals(5, ar.length);
        for (int i = 0; i < 5; ++i) {
            assertTrue(map.containsKey(((Map.Entry) (ar[i])).getKey()));
            assertTrue(map.containsValue(((Map.Entry) (ar[i])).getValue()));
        }
    }

    /**
     * Maps with same contents are equal
     */
    @Test
    public void testEquals() {
        IncrementalSortMap<Integer, String> map1 = map5();
        IncrementalSortMap<Integer, String> map2 = map5();
        assertEquals(map1, map2);
        assertEquals(map2, map1);
        map1.clear();
        assertFalse(map1.equals(map2));
        assertFalse(map2.equals(map1));
    }

    /**
     * firstKey returns first key
     */
    @Test
    public void testFirstKey() {
        IncrementalSortMap<Integer, String> map = map5();
        assertEquals(one, map.firstKey());
    }

    /**
     * floorEntry returns preceding entry.
     */
    @Test
    public void testFloorEntry() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e1 = map.floorEntry(three);
        assertEquals(three, e1.getKey());

        Map.Entry e2 = map.floorEntry(six);
        assertEquals(five, e2.getKey());

        Map.Entry e3 = map.floorEntry(one);
        assertEquals(one, e3.getKey());

        Map.Entry e4 = map.floorEntry(zero);
        assertNull(e4);
    }

    /**
     * floorKey returns preceding element
     */
    @Test
    public void testFloorKey() {
        IncrementalSortMap<Integer, String> q = map5();
        Object e1 = q.floorKey(three);
        assertEquals(three, e1);

        Object e2 = q.floorKey(six);
        assertEquals(five, e2);

        Object e3 = q.floorKey(one);
        assertEquals(one, e3);

        Object e4 = q.floorKey(zero);
        assertNull(e4);
    }

    /**
     * get returns the correct element at the given key,
     * or null if not present
     */
    @Test
    public void testGet() {
        IncrementalSortMap<Integer, String> map = map5();
        assertEquals("A", (String) map.get(one));
        IncrementalSortMap<Integer, String> empty = IncrementalSortMap.create();
        assertNull(empty.get(one));
    }

    /**
     * get(null) of nonempty map throws NPE
     */
    @Test
    public void testGet_NullPointerException() {
        try {
            IncrementalSortMap<Integer, String> c = map5();
            c.get(null);
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * headMap returns map with keys in requested range
     */
    @Test
    public void testHeadMapContents() {
        IncrementalSortMap<Integer, String> map = map5();
        NavigableMap sm = map.headMap(four, false);
        assertTrue(sm.containsKey(one));
        assertTrue(sm.containsKey(two));
        assertTrue(sm.containsKey(three));
        assertFalse(sm.containsKey(four));
        assertFalse(sm.containsKey(five));
        Iterator i = sm.keySet().iterator();
        Object k;
        k = (Integer) (i.next());
        assertEquals(one, k);
        k = (Integer) (i.next());
        assertEquals(two, k);
        k = (Integer) (i.next());
        assertEquals(three, k);
        assertFalse(i.hasNext());
        sm.clear();
        assertTrue(sm.isEmpty());
        assertEquals(2, map.size());
        assertEquals(four, map.firstKey());
    }

    /**
     * higherEntry returns next entry.
     */
    @Test
    public void testHigherEntry() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e1 = map.higherEntry(three);
        assertEquals(four, e1.getKey());

        Map.Entry e2 = map.higherEntry(zero);
        assertEquals(one, e2.getKey());

        Map.Entry e3 = map.higherEntry(five);
        assertNull(e3);

        Map.Entry e4 = map.higherEntry(six);
        assertNull(e4);
    }

    /**
     * higherKey returns next element
     */
    @Test
    public void testHigherKey() {
        IncrementalSortMap<Integer, String> q = map5();
        Object e1 = q.higherKey(three);
        assertEquals(four, e1);

        Object e2 = q.higherKey(zero);
        assertEquals(one, e2);

        Object e3 = q.higherKey(five);
        assertNull(e3);

        Object e4 = q.higherKey(six);
        assertNull(e4);
    }

    /**
     * isEmpty is true of empty map and false for non-empty
     */
    @Test
    public void testIsEmpty() {
        IncrementalSortMap<Integer, String> empty = IncrementalSortMap.create();
        IncrementalSortMap<Integer, String> map = map5();
        assertTrue(empty.isEmpty());
        assertFalse(map.isEmpty());
    }

    /**
     * keySet returns a Set containing all the keys
     */
    @Test
    public void testKeySet() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.keySet();
        assertEquals(5, s.size());
        assertTrue(s.contains(one));
        assertTrue(s.contains(two));
        assertTrue(s.contains(three));
        assertTrue(s.contains(four));
        assertTrue(s.contains(five));
    }

    /**
     * descending iterator of key set is inverse ordered
     */
    @Test
    public void testKeySetDescendingIteratorOrder() {
        IncrementalSortMap<Integer, String> map = map5();
        NavigableSet s = map.navigableKeySet();
        Iterator i = s.descendingIterator();
        Integer last = (Integer) i.next();
        assertEquals(last, five);
        int count = 1;
        while (i.hasNext()) {
            Integer k = (Integer) i.next();
            assertTrue(last.compareTo(k) > 0);
            last = k;
            ++count;
        }
        assertEquals(5, count);
    }

    /**
     * keySet is ordered
     */
    @Test
    public void testKeySetOrder() {
        IncrementalSortMap<Integer, String> map = map5();
        Set s = map.keySet();
        Iterator i = s.iterator();
        Integer last = (Integer) i.next();
        assertEquals(last, one);
        int count = 1;
        while (i.hasNext()) {
            Integer k = (Integer) i.next();
            assertTrue(last.compareTo(k) < 0);
            last = k;
            ++count;
        }
        assertEquals(5, count);
    }

    /**
     * lastKey returns last key
     */
    @Test
    public void testLastKey() {
        IncrementalSortMap<Integer, String> map = map5();
        assertEquals(five, map.lastKey());
    }

    /**
     * lowerEntry returns preceding entry.
     */
    @Test
    public void testLowerEntry() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e1 = map.lowerEntry(three);
        assertEquals(two, e1.getKey());

        Map.Entry e2 = map.lowerEntry(six);
        assertEquals(five, e2.getKey());

        Map.Entry e3 = map.lowerEntry(one);
        assertNull(e3);

        Map.Entry e4 = map.lowerEntry(zero);
        assertNull(e4);
    }

    /**
     * lowerKey returns preceding element
     */
    @Test
    public void testLowerKey() {
        IncrementalSortMap<Integer, String> q = map5();
        Object e1 = q.lowerKey(three);
        assertEquals(two, e1);

        Object e2 = q.lowerKey(six);
        assertEquals(five, e2);

        Object e3 = q.lowerKey(one);
        assertNull(e3);

        Object e4 = q.lowerKey(zero);
        assertNull(e4);
    }

    /**
     * pollFirstEntry returns entries in order
     */
    @Test
    public void testPollFirstEntry() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e = map.pollFirstEntry();
        assertEquals(one, e.getKey());
        assertEquals("A", e.getValue());
        e = map.pollFirstEntry();
        assertEquals(two, e.getKey());
        map.put(one, "A");
        e = map.pollFirstEntry();
        assertEquals(one, e.getKey());
        assertEquals("A", e.getValue());
        e = map.pollFirstEntry();
        assertEquals(three, e.getKey());
        map.remove(four);
        e = map.pollFirstEntry();
        assertEquals(five, e.getKey());
        try {
            e.setValue("A");
            shouldThrow();
        }
        catch (UnsupportedOperationException success) {}
        e = map.pollFirstEntry();
        assertNull(e);
    }

    /**
     * pollLastEntry returns entries in order
     */
    @Test
    public void testPollLastEntry() {
        IncrementalSortMap<Integer, String> map = map5();
        Map.Entry e = map.pollLastEntry();
        assertEquals(five, e.getKey());
        assertEquals("E", e.getValue());
        e = map.pollLastEntry();
        assertEquals(four, e.getKey());
        map.put(five, "E");
        e = map.pollLastEntry();
        assertEquals(five, e.getKey());
        assertEquals("E", e.getValue());
        e = map.pollLastEntry();
        assertEquals(three, e.getKey());
        map.remove(two);
        e = map.pollLastEntry();
        assertEquals(one, e.getKey());
        try {
            e.setValue("E");
            shouldThrow();
        }
        catch (UnsupportedOperationException success) {}
        e = map.pollLastEntry();
        assertNull(e);
    }

    /**
     * put(null,x) throws NPE
     */
    @Test
    public void testPut1_NullPointerException() {
        try {
            IncrementalSortMap<Integer, String> c = map5();
            c.put(null, "whatever");
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    @Test
    public void testPutAll() {
        IncrementalSortMap<Integer, String> empty = IncrementalSortMap.create();
        IncrementalSortMap<Integer, String> map = map5();
        empty.putAll(map);
        assertEquals(5, empty.size());
        assertTrue(empty.containsKey(one));
        assertTrue(empty.containsKey(two));
        assertTrue(empty.containsKey(three));
        assertTrue(empty.containsKey(four));
        assertTrue(empty.containsKey(five));
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Test
    public void testPutIfAbsent() {
        IncrementalSortMap<Integer, String> map = map5();
        map.putIfAbsent(six, "Z");
        assertTrue(map.containsKey(six));
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test
    public void testPutIfAbsent1_NullPointerException() {
        try {
            IncrementalSortMap<Integer, String> c = map5();
            c.putIfAbsent(null, "whatever");
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    // Exception tests

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Test
    public void testPutIfAbsent2() {
        IncrementalSortMap<Integer, String> map = map5();
        assertEquals("A", map.putIfAbsent(one, "Z"));
    }

    /**
     * Submaps of submaps subdivide correctly
     */
    @Test
    public void testRecursiveSubMaps() throws Exception {
        int mapSize = expensiveTests ? 1000 : 100;
        NavigableMap<Integer, Integer> map = IncrementalSortMap.create();
        bs = new BitSet(mapSize);

        populate(map, mapSize);
        check(map, 0, mapSize - 1, true);
        check(map.descendingMap(), 0, mapSize - 1, false);

        mutateMap(map, 0, mapSize - 1);
        check(map, 0, mapSize - 1, true);
        check(map.descendingMap(), 0, mapSize - 1, false);

        bashSubMap(map.subMap(0, true, mapSize, false), 0, mapSize - 1, true);
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test
    public void testRemove() {
        IncrementalSortMap<Integer, String> map = map5();
        map.remove(five);
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
    }

    /**
     * remove(null) throws NPE
     */
    @Test
    public void testRemove1_NullPointerException() {
        try {
            IncrementalSortMap<String, String> c = IncrementalSortMap.create();
            c.put("sadsdf", "asdads");
            c.remove(null);
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test
    public void testRemove2() {
        IncrementalSortMap<Integer, String> map = map5();
        assertTrue(map.containsKey(five));
        assertEquals("E", map.get(five));
        map.remove(five, "E");
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
        map.remove(four, "A");
        assertEquals(4, map.size());
        assertTrue(map.containsKey(four));
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test
    public void testRemove2_NullPointerException() {
        try {
            IncrementalSortMap<String, String> c = IncrementalSortMap.create();
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * remove(x, null) returns false
     */
    @Test
    public void testRemove3() {
        IncrementalSortMap<String, String> c = IncrementalSortMap.create();
        c.put("sadsdf", "asdads");
        assertFalse(c.remove("sadsdf", null));
    }

    /**
     * replace fails when the given key is not present
     */
    @Test
    public void testReplace() {
        IncrementalSortMap<Integer, String> map = map5();
        assertNull(map.replace(six, "Z"));
        assertFalse(map.containsKey(six));
    }

    /**
     * replace(null, x) throws NPE
     */
    @Test
    public void testReplace_NullPointerException() {
        try {
            IncrementalSortMap<Integer, String> c = map5();
            c.replace(null, "whatever");
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test
    public void testReplace2() {
        IncrementalSortMap<Integer, String> map = map5();
        assertNotNull(map.replace(one, "Z"));
        assertEquals("Z", map.get(one));
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test
    public void testReplaceValue() {
        IncrementalSortMap<Integer, String> map = map5();
        assertEquals("A", map.get(one));
        assertFalse(map.replace(one, "Z", "Z"));
        assertEquals("A", map.get(one));
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test
    public void testReplaceValue2() {
        IncrementalSortMap<Integer, String> map = map5();
        assertEquals("A", map.get(one));
        assertTrue(map.replace(one, "A", "Z"));
        assertEquals("Z", map.get(one));
    }

    /**
     * size returns the correct values
     */
    @Test
    public void testSize() {
        IncrementalSortMap<Integer, String> map = map5();
        IncrementalSortMap<Integer, String> empty = IncrementalSortMap.create();
        assertEquals(0, empty.size());
        assertEquals(5, map.size());
    }

    /**
     * subMap returns map with keys in requested range
     */
    @Test
    public void testSubMapContents() {
        IncrementalSortMap<Integer, String> map = map5();
        NavigableMap sm = map.subMap(two, true, four, false);
        assertEquals(two, sm.firstKey());
        assertEquals(three, sm.lastKey());
        assertEquals(2, sm.size());
        assertFalse(sm.containsKey(one));
        assertTrue(sm.containsKey(two));
        assertTrue(sm.containsKey(three));
        assertFalse(sm.containsKey(four));
        assertFalse(sm.containsKey(five));
        Iterator i = sm.keySet().iterator();
        Object k;
        k = (Integer) (i.next());
        assertEquals(two, k);
        k = (Integer) (i.next());
        assertEquals(three, k);
        assertFalse(i.hasNext());
        Iterator r = sm.descendingKeySet().iterator();
        k = (Integer) (r.next());
        assertEquals(three, k);
        k = (Integer) (r.next());
        assertEquals(two, k);
        assertFalse(r.hasNext());

        Iterator j = sm.keySet().iterator();
        j.next();
        j.remove();
        assertFalse(map.containsKey(two));
        assertEquals(4, map.size());
        assertEquals(1, sm.size());
        assertEquals(three, sm.firstKey());
        assertEquals(three, sm.lastKey());
        assertEquals("C", sm.remove(three));
        assertTrue(sm.isEmpty());
        assertEquals(3, map.size());
    }

    @Test
    public void testSubMapContents2() {
        IncrementalSortMap<Integer, String> map = map5();
        NavigableMap sm = map.subMap(two, true, three, false);
        assertEquals(1, sm.size());
        assertEquals(two, sm.firstKey());
        assertEquals(two, sm.lastKey());
        assertFalse(sm.containsKey(one));
        assertTrue(sm.containsKey(two));
        assertFalse(sm.containsKey(three));
        assertFalse(sm.containsKey(four));
        assertFalse(sm.containsKey(five));
        Iterator i = sm.keySet().iterator();
        Object k;
        k = (Integer) (i.next());
        assertEquals(two, k);
        assertFalse(i.hasNext());
        Iterator r = sm.descendingKeySet().iterator();
        k = (Integer) (r.next());
        assertEquals(two, k);
        assertFalse(r.hasNext());

        Iterator j = sm.keySet().iterator();
        j.next();
        j.remove();
        assertFalse(map.containsKey(two));
        assertEquals(4, map.size());
        assertEquals(0, sm.size());
        assertTrue(sm.isEmpty());
        assertSame(sm.remove(three), null);
        assertEquals(4, map.size());
    }

    /**
     * tailMap returns map with keys in requested range
     */
    @Test
    public void testTailMapContents() {
        IncrementalSortMap<Integer, String> map = map5();
        NavigableMap sm = map.tailMap(two, true);
        assertFalse(sm.containsKey(one));
        assertTrue(sm.containsKey(two));
        assertTrue(sm.containsKey(three));
        assertTrue(sm.containsKey(four));
        assertTrue(sm.containsKey(five));
        Iterator i = sm.keySet().iterator();
        Object k;
        k = (Integer) (i.next());
        assertEquals(two, k);
        k = (Integer) (i.next());
        assertEquals(three, k);
        k = (Integer) (i.next());
        assertEquals(four, k);
        k = (Integer) (i.next());
        assertEquals(five, k);
        assertFalse(i.hasNext());
        Iterator r = sm.descendingKeySet().iterator();
        k = (Integer) (r.next());
        assertEquals(five, k);
        k = (Integer) (r.next());
        assertEquals(four, k);
        k = (Integer) (r.next());
        assertEquals(three, k);
        k = (Integer) (r.next());
        assertEquals(two, k);
        assertFalse(r.hasNext());

        Iterator ei = sm.entrySet().iterator();
        Map.Entry e;
        e = (Map.Entry) (ei.next());
        assertEquals(two, e.getKey());
        assertEquals("B", e.getValue());
        e = (Map.Entry) (ei.next());
        assertEquals(three, e.getKey());
        assertEquals("C", e.getValue());
        e = (Map.Entry) (ei.next());
        assertEquals(four, e.getKey());
        assertEquals("D", e.getValue());
        e = (Map.Entry) (ei.next());
        assertEquals(five, e.getKey());
        assertEquals("E", e.getValue());
        assertFalse(i.hasNext());

        NavigableMap ssm = sm.tailMap(four, true);
        assertEquals(four, ssm.firstKey());
        assertEquals(five, ssm.lastKey());
        assertEquals("D", ssm.remove(four));
        assertEquals(1, ssm.size());
        assertEquals(3, sm.size());
        assertEquals(4, map.size());
    }

    /**
     * toString contains toString of elements
     */
    @Test
    public void testToString() {
        IncrementalSortMap<Integer, String> map = map5();
        String s = map.toString();
        for (int i = 1; i <= 5; ++i) {
            assertTrue(s.contains(String.valueOf(i)));
        }
    }

    /**
     * values collection contains all values
     */
    @Test
    public void testValues() {
        IncrementalSortMap<Integer, String> map = map5();
        Collection s = map.values();
        assertEquals(5, s.size());
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    /**
     * Values.toArray contains all values
     */
    @Test
    public void testValuesToArray() {
        IncrementalSortMap<Integer, String> map = map5();
        Collection v = map.values();
        Object[] ar = v.toArray();
        ArrayList s = new ArrayList(Arrays.asList(ar));
        assertEquals(5, ar.length);
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    private void shouldThrow() {
        throw new RuntimeException("Test exception");
    }

    void bashSubMap(NavigableMap<Integer, Integer> map, int min, int max,
            boolean ascending) {
        check(map, min, max, ascending);
        check(map.descendingMap(), min, max, !ascending);

        mutateSubMap(map, min, max);
        check(map, min, max, ascending);
        check(map.descendingMap(), min, max, !ascending);

        // Recurse
        if(max - min < 2)
            return;
        int midPoint = (min + max) / 2;

        // headMap - pick direction and endpoint inclusion randomly
        boolean incl = rnd.nextBoolean();
        NavigableMap<Integer, Integer> hm = map.headMap(midPoint, incl);
        if(ascending) {
            if(rnd.nextBoolean())
                bashSubMap(hm, min, midPoint - (incl ? 0 : 1), true);
            else
                bashSubMap(hm.descendingMap(), min, midPoint - (incl ? 0 : 1),
                        false);
        }
        else {
            if(rnd.nextBoolean())
                bashSubMap(hm, midPoint + (incl ? 0 : 1), max, false);
            else
                bashSubMap(hm.descendingMap(), midPoint + (incl ? 0 : 1), max,
                        true);
        }

        // tailMap - pick direction and endpoint inclusion randomly
        incl = rnd.nextBoolean();
        NavigableMap<Integer, Integer> tm = map.tailMap(midPoint, incl);
        if(ascending) {
            if(rnd.nextBoolean())
                bashSubMap(tm, midPoint + (incl ? 0 : 1), max, true);
            else
                bashSubMap(tm.descendingMap(), midPoint + (incl ? 0 : 1), max,
                        false);
        }
        else {
            if(rnd.nextBoolean()) {
                bashSubMap(tm, min, midPoint - (incl ? 0 : 1), false);
            }
            else {
                bashSubMap(tm.descendingMap(), min, midPoint - (incl ? 0 : 1),
                        true);
            }
        }

        // subMap - pick direction and endpoint inclusion randomly
        int rangeSize = max - min + 1;
        int[] endpoints = new int[2];
        endpoints[0] = min + rnd.nextInt(rangeSize);
        endpoints[1] = min + rnd.nextInt(rangeSize);
        Arrays.sort(endpoints);
        boolean lowIncl = rnd.nextBoolean();
        boolean highIncl = rnd.nextBoolean();
        if(ascending) {
            NavigableMap<Integer, Integer> sm = map.subMap(endpoints[0],
                    lowIncl, endpoints[1], highIncl);
            if(rnd.nextBoolean())
                bashSubMap(sm, endpoints[0] + (lowIncl ? 0 : 1), endpoints[1]
                        - (highIncl ? 0 : 1), true);
            else
                bashSubMap(sm.descendingMap(),
                        endpoints[0] + (lowIncl ? 0 : 1), endpoints[1]
                                - (highIncl ? 0 : 1), false);
        }
        else {
            NavigableMap<Integer, Integer> sm = map.subMap(endpoints[1],
                    highIncl, endpoints[0], lowIncl);
            if(rnd.nextBoolean())
                bashSubMap(sm, endpoints[0] + (lowIncl ? 0 : 1), endpoints[1]
                        - (highIncl ? 0 : 1), false);
            else
                bashSubMap(sm.descendingMap(),
                        endpoints[0] + (lowIncl ? 0 : 1), endpoints[1]
                                - (highIncl ? 0 : 1), true);
        }
    }

    /**
     * min and max are both inclusive. If max < min, interval is empty.
     */
    void check(NavigableMap<Integer, Integer> map, final int min,
            final int max, final boolean ascending) {
        class ReferenceSet {
            private int firstAscending() {
                int result = ceilingAscending(min);
                return result > max ? -1 : result;
            }

            private int lastAscending() {
                int result = floorAscending(max);
                return result < min ? -1 : result;
            }

            int ceiling(int key) {
                return ascending ? ceilingAscending(key) : floorAscending(key);
            }

            int ceilingAscending(int key) {
                if(key < min)
                    key = min;
                else if(key > max)
                    return -1;
                int result = bs.nextSetBit(key);
                return result > max ? -1 : result;
            }

            int first() {
                return ascending ? firstAscending() : lastAscending();
            }

            int floor(int key) {
                return ascending ? floorAscending(key) : ceilingAscending(key);
            }

            int floorAscending(int key) {
                if(key < min)
                    return -1;
                else if(key > max)
                    key = max;

                // BitSet should support this! Test would run much faster
                while (key >= min) {
                    if(bs.get(key))
                        return key;
                    key--;
                }
                return -1;
            }

            int higher(int key) {
                return ascending ? higherAscending(key) : lowerAscending(key);
            }

            int higherAscending(int key) {
                return ceilingAscending(key + 1);
            }

            int last() {
                return ascending ? lastAscending() : firstAscending();
            }

            int lower(int key) {
                return ascending ? lowerAscending(key) : higherAscending(key);
            }

            int lowerAscending(int key) {
                return floorAscending(key - 1);
            }
        }
        ReferenceSet rs = new ReferenceSet();

        // Test contents using containsKey
        int size = 0;
        for (int i = min; i <= max; i++) {
            boolean bsContainsI = bs.get(i);
            assertEquals(bsContainsI, map.containsKey(i));
            if(bsContainsI)
                size++;
        }
        assertEquals(size, map.size());

        // Test contents using contains keySet iterator
        int size2 = 0;
        int previousKey = -1;
        for (int key : map.keySet()) {
            assertTrue(bs.get(key));
            size2++;
            assertTrue(previousKey < 0
                    || (ascending ? key - previousKey > 0
                            : key - previousKey < 0));
            previousKey = key;
        }
        assertEquals(size2, size);

        // Test navigation ops
        for (int key = min - 1; key <= max + 1; key++) {
            assertEq(map.lowerKey(key), rs.lower(key));
            assertEq(map.floorKey(key), rs.floor(key));
            assertEq(map.higherKey(key), rs.higher(key));
            assertEq(map.ceilingKey(key), rs.ceiling(key));
        }

        // Test extrema
        if(map.size() != 0) {
            assertEq(map.firstKey(), rs.first());
            assertEq(map.lastKey(), rs.last());
        }
        else {
            assertEq(rs.first(), -1);
            assertEq(rs.last(), -1);
            try {
                map.firstKey();
                shouldThrow();
            }
            catch (NoSuchElementException success) {}
            try {
                map.lastKey();
                shouldThrow();
            }
            catch (NoSuchElementException success) {}
        }
    }

    void mutateMap(NavigableMap<Integer, Integer> map, int min, int max) {
        int size = map.size();
        int rangeSize = max - min + 1;

        // Remove a bunch of entries directly
        for (int i = 0, n = rangeSize / 2; i < n; i++) {
            remove(map, min - 5 + rnd.nextInt(rangeSize + 10));
        }

        // Remove a bunch of entries with iterator
        for (Iterator<Integer> it = map.keySet().iterator(); it.hasNext();) {
            if(rnd.nextBoolean()) {
                bs.clear(it.next());
                it.remove();
            }
        }

        // Add entries till we're back to original size
        while (map.size() < size) {
            int key = min + rnd.nextInt(rangeSize);
            assertTrue(key >= min && key <= max);
            put(map, key);
        }
    }

    void mutateSubMap(NavigableMap<Integer, Integer> map, int min, int max) {
        int size = map.size();
        int rangeSize = max - min + 1;

        // Remove a bunch of entries directly
        for (int i = 0, n = rangeSize / 2; i < n; i++) {
            remove(map, min - 5 + rnd.nextInt(rangeSize + 10));
        }

        // Remove a bunch of entries with iterator
        for (Iterator<Integer> it = map.keySet().iterator(); it.hasNext();) {
            if(rnd.nextBoolean()) {
                bs.clear(it.next());
                it.remove();
            }
        }

        // Add entries till we're back to original size
        while (map.size() < size) {
            int key = min - 5 + rnd.nextInt(rangeSize + 10);
            if(key >= min && key <= max) {
                put(map, key);
            }
            else {
                try {
                    map.put(key, 2 * key);
                    shouldThrow();
                }
                catch (IllegalArgumentException success) {}
            }
        }
    }

    void populate(NavigableMap<Integer, Integer> map, int limit) {
        for (int i = 0, n = 2 * limit / 3; i < n; i++) {
            int key = rnd.nextInt(limit);
            put(map, key);
        }
    }

    void put(NavigableMap<Integer, Integer> map, int key) {
        if(map.put(key, 2 * key) == null)
            bs.set(key);
    }

    void remove(NavigableMap<Integer, Integer> map, int key) {
        if(map.remove(key) != null)
            bs.clear(key);
    }

}
