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
package com.google.common.collect;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;

/**
 * Tests for {@code Multimap} implementations. Caution: when subclassing avoid
 * accidental naming collisions with tests in this class!
 * 
 * @author Jared Levy
 * @author Jeff Nelson
 */
public abstract class AbstractMultimapTest extends ConcourseBaseTest {

    private Multimap<String, Integer> multimap;

    protected abstract Multimap<String, Integer> create();

    protected Multimap<String, Integer> createSample() {
        Multimap<String, Integer> sample = create();
        sample.putAll("foo", asList(3, -1, 2, 4, 1));
        sample.putAll("bar", asList(1, 2, 3, 1));
        return sample;
    }

    @Override
    protected void beforeEachTest() {
        multimap = create();
    }

    protected Multimap<String, Integer> getMultimap() {
        return multimap;
    }

    /**
     * Returns the key to use as a null placeholder in tests. The default
     * implementation returns {@code null}, but tests for multimaps that don't
     * support null keys should override it.
     */
    protected String nullKey() {
        return null;
    }

    /**
     * Returns the value to use as a null placeholder in tests. The default
     * implementation returns {@code null}, but tests for multimaps that don't
     * support null values should override it.
     */
    protected Integer nullValue() {
        return null;
    }

    /**
     * Validate multimap size by calling {@code size()} and also by iterating
     * through the entries. This tests cases where the {@code entries()} list is
     * stored separately, such as the {@link LinkedHashMultimap}. It also
     * verifies that the multimap contains every multimap entry.
     */
    protected void assertSize(int expectedSize) {
        assertEquals(expectedSize, multimap.size());

        int size = 0;
        for (Entry<String, Integer> entry : multimap.entries()) {
            assertTrue(
                    multimap.containsEntry(entry.getKey(), entry.getValue()));
            size++;
        }
        assertEquals(expectedSize, size);

        int size2 = 0;
        for (Entry<String, Collection<Integer>> entry2 : multimap.asMap()
                .entrySet()) {
            size2 += entry2.getValue().size();
        }
        assertEquals(expectedSize, size2);
    }

    @Test
    public void testSize0() {
        assertSize(0);
    }

    @Test
    public void testSize1() {
        multimap.put("foo", 1);
        assertSize(1);
    }

    @Test
    public void testSize2Keys() {
        multimap.put("foo", 1);
        multimap.put("bar", 5);
        assertSize(2);
    }

    @Test
    public void testSize2Values() {
        multimap.put("foo", 1);
        multimap.put("foo", 7);
        assertSize(2);
    }

    @Test
    public void testSizeNull() {
        multimap.put("foo", 1);
        multimap.put("bar", 5);
        multimap.put(nullKey(), nullValue());
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 5);
        assertSize(5);
    }

    @Test
    public void testIsEmptyYes() {
        assertTrue(multimap.isEmpty());
    }

    @Test
    public void testIsEmptyNo() {
        multimap.put("foo", 1);
        assertFalse(multimap.isEmpty());
    }

    @Test
    public void testIsEmptyNull() {
        multimap.put(nullKey(), nullValue());
        assertFalse(multimap.isEmpty());
    }

    @Test
    public void testIsEmptyRemoved() {
        multimap.put("foo", 1);
        multimap.remove("foo", 1);
        assertTrue(multimap.isEmpty());
    }

    @Test
    public void testContainsKeyTrue() {
        multimap.put("foo", 1);
        assertTrue(multimap.containsKey("foo"));
    }

    @Test
    public void testContainsKeyFalse() {
        multimap.put("foo", 1);
        assertFalse(multimap.containsKey("bar"));
        assertFalse(multimap.containsKey(nullKey()));
    }

    @Test
    public void testContainsKeyNull() {
        multimap.put(nullKey(), 1);
        assertTrue(multimap.containsKey(nullKey()));
    }

    @Test
    public void testContainsValueTrue() {
        multimap.put("foo", 1);
        assertTrue(multimap.containsValue(1));
    }

    @Test
    public void testContainsValueFalse() {
        multimap.put("foo", 1);
        assertFalse(multimap.containsValue(2));
        assertFalse(multimap.containsValue(nullValue()));
    }

    @Test
    public void testContainsValueNull() {
        multimap.put("foo", nullValue());
        assertTrue(multimap.containsValue(nullValue()));
    }

    @Test
    public void testContainsKeyValueTrue() {
        multimap.put("foo", 1);
        assertTrue(multimap.containsEntry("foo", 1));
    }

    @Test
    public void testContainsKeyValueRemoved() {
        multimap.put("foo", 1);
        multimap.remove("foo", 1);
        assertFalse(multimap.containsEntry("foo", 1));
    }

    @Test
    public void testGet0() {
        multimap.put("foo", 1);
        Collection<Integer> values = multimap.get("bar");
        assertEquals(0, values.size());
    }

    @Test
    public void testGet1() {
        multimap.put("foo", 1);
        multimap.put("bar", 3);
        Collection<Integer> values = multimap.get("bar");
        assertEquals(1, values.size());
        assertTrue(values.contains(3));
        assertFalse(values.contains(5));
    }

    @Test
    public void testGet2() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        Collection<Integer> values = multimap.get("foo");
        assertEquals(2, values.size());
        assertTrue(values.contains(1));
        assertTrue(values.contains(3));
    }

    @Test
    public void testGetNull() {
        multimap.put(nullKey(), nullValue());
        multimap.put(nullKey(), 3);
        Collection<Integer> values = multimap.get(nullKey());
        assertEquals(2, values.size());
        assertTrue(values.contains(nullValue()));
        assertTrue(values.contains(3));
    }

    @Test
    public void testPutAllIterable() {
        Iterable<Integer> iterable = new Iterable<Integer>() {
            public Iterator<Integer> iterator() {
                return Lists.newArrayList(1, 3).iterator();
            }
        };
        multimap.putAll("foo", iterable);
        assertTrue(multimap.containsEntry("foo", 1));
        assertTrue(multimap.containsEntry("foo", 3));
        assertSize(2);

        Iterable<Integer> emptyIterable = Lists.<Integer> newArrayList();
        multimap.putAll("bar", emptyIterable);
        assertSize(2);
        assertEquals(Collections.singleton("foo"), multimap.keySet());
    }

    @Test
    public void testPutAllCollection() {
        Collection<Integer> collection = Lists.newArrayList(1, 3);
        multimap.putAll("foo", collection);
        assertTrue(multimap.containsEntry("foo", 1));
        assertTrue(multimap.containsEntry("foo", 3));
        assertSize(2);

        Collection<Integer> emptyCollection = Lists.newArrayList();
        multimap.putAll("bar", emptyCollection);
        assertSize(2);
        assertEquals(Collections.singleton("foo"), multimap.keySet());
    }

    @Test
    public void testPutAllCollectionNull() {
        Collection<Integer> collection = Lists.newArrayList(1, nullValue());
        multimap.putAll(nullKey(), collection);
        assertTrue(multimap.containsEntry(nullKey(), 1));
        assertTrue(multimap.containsEntry(nullKey(), nullValue()));
        assertSize(2);
    }

    @Test
    public void testPutAllEmptyCollection() {
        Collection<Integer> collection = Lists.newArrayList();
        multimap.putAll("foo", collection);
        assertSize(0);
        assertTrue(multimap.isEmpty());
    }

    @Test
    public void testPutAllMultimap() {
        multimap.put("foo", 2);
        multimap.put("cow", 5);
        multimap.put(nullKey(), 2);
        Multimap<String, Integer> multimap2 = create();
        multimap2.put("foo", 1);
        multimap2.put("bar", 3);
        multimap2.put(nullKey(), nullValue());
        multimap.putAll(multimap2);
        assertTrue(multimap.containsEntry("foo", 2));
        assertTrue(multimap.containsEntry("cow", 5));
        assertTrue(multimap.containsEntry("foo", 1));
        assertTrue(multimap.containsEntry("bar", 3));
        assertTrue(multimap.containsEntry(nullKey(), nullValue()));
        assertTrue(multimap.containsEntry(nullKey(), 2));
        assertSize(6);
    }

    @Test
    public void testPutAllReturn_emptyCollection() {
        assertFalse(multimap.putAll("foo", new ArrayList<Integer>()));
        assertFalse(multimap.putAll(create()));
    }

    @Test
    public void testPutAllReturn_nonEmptyCollection() {
        assertTrue(multimap.putAll("foo", asList(1, 2, 3)));
        assertTrue(multimap.putAll("foo", asList(4, 5, 6)));
        assertFalse(multimap.putAll(create()));

        Multimap<String, Integer> other = create();
        other.putAll("bar", asList(7, 8, 9));
        assertTrue(multimap.putAll(other));
    }

    @Test
    public void testReplaceValues() {
        multimap.put("foo", 1);
        multimap.put("bar", 3);
        Collection<Integer> values = asList(2, nullValue());
        Collection<Integer> oldValues = multimap.replaceValues("foo", values);
        assertTrue(multimap.containsEntry("foo", 2));
        assertTrue(multimap.containsEntry("foo", nullValue()));
        assertTrue(multimap.containsEntry("bar", 3));
        assertSize(3);
        assertTrue(oldValues.contains(1));
        assertEquals(1, oldValues.size());
    }

    @Test
    public void testReplaceValuesEmpty() {
        multimap.put("foo", 1);
        multimap.put("bar", 3);
        // "<Integer>" for javac 1.5.
        Collection<Integer> values = Arrays.<Integer> asList();
        Collection<Integer> oldValues = multimap.replaceValues("foo", values);
        assertFalse(multimap.containsKey("foo"));
        assertTrue(multimap.containsEntry("bar", 3));
        assertSize(1);
        assertTrue(oldValues.contains(1));
        assertEquals(1, oldValues.size());
    }

    @Test
    public void testReplaceValuesNull() {
        multimap.put(nullKey(), 1);
        multimap.put("bar", 3);
        Collection<Integer> values = asList(2, nullValue());
        Collection<Integer> oldValues = multimap.replaceValues(nullKey(),
                values);
        assertTrue(multimap.containsEntry(nullKey(), 2));
        assertTrue(multimap.containsEntry(nullKey(), nullValue()));
        assertTrue(multimap.containsEntry("bar", 3));
        assertSize(3);
        assertTrue(oldValues.contains(1));
        assertEquals(1, oldValues.size());
    }

    @Test
    public void testReplaceValuesNotPresent() {
        multimap.put("bar", 3);
        Collection<Integer> values = asList(2, 4);
        Collection<Integer> oldValues = multimap.replaceValues("foo", values);
        assertTrue(multimap.containsEntry("foo", 2));
        assertTrue(multimap.containsEntry("foo", 4));
        assertTrue(multimap.containsEntry("bar", 3));
        assertSize(3);
        assertNotNull(oldValues);
        assertTrue(oldValues.isEmpty());
    }

    @Test
    public void testReplaceValuesDuplicates() {
        Collection<Integer> values = Lists.newArrayList(1, 2, 3, 2, 1);
        multimap.put("bar", 3);
        Collection<Integer> oldValues = multimap.replaceValues("bar", values);
        Collection<Integer> replacedValues = multimap.get("bar");
        assertSize(multimap.size());
        assertEquals(replacedValues.size(), multimap.size());
        assertEquals(1, oldValues.size());
        assertTrue(oldValues.contains(3));
    }

    @Test
    public void testRemove() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);

        assertTrue(multimap.remove("foo", 1));
        assertFalse(multimap.containsEntry("foo", 1));
        assertTrue(multimap.containsEntry("foo", 3));
        assertSize(1);

        assertFalse(multimap.remove("bar", 3));
        assertTrue(multimap.containsEntry("foo", 3));
        assertSize(1);

        assertFalse(multimap.remove("foo", 2));
        assertTrue(multimap.containsEntry("foo", 3));
        assertSize(1);

        assertTrue(multimap.remove("foo", 3));
        assertFalse(multimap.containsKey("foo"));
        assertSize(0);
    }

    @Test
    public void testRemoveNull() {
        multimap.put(nullKey(), 1);
        multimap.put(nullKey(), 3);
        multimap.put(nullKey(), nullValue());

        assertTrue(multimap.remove(nullKey(), 1));
        assertFalse(multimap.containsEntry(nullKey(), 1));
        assertTrue(multimap.containsEntry(nullKey(), 3));
        assertTrue(multimap.containsEntry(nullKey(), nullValue()));
        assertSize(2);

        assertTrue(multimap.remove(nullKey(), nullValue()));
        assertFalse(multimap.containsEntry(nullKey(), 1));
        assertTrue(multimap.containsEntry(nullKey(), 3));
        assertFalse(multimap.containsEntry(nullKey(), nullValue()));
        assertSize(1);
    }

    @Test
    public void testRemoveAll() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        Collection<Integer> removed = multimap.removeAll("foo");
        assertFalse(multimap.containsKey("foo"));
        assertSize(0);
        assertTrue(removed.contains(1));
        assertTrue(removed.contains(3));
        assertEquals(2, removed.size());
    }

    @Test
    public void testRemoveAllNull() {
        multimap.put(nullKey(), 1);
        multimap.put(nullKey(), nullValue());
        Collection<Integer> removed = multimap.removeAll(nullKey());
        assertFalse(multimap.containsKey(nullKey()));
        assertSize(0);
        assertTrue(removed.contains(1));
        assertTrue(removed.contains(nullValue()));
        assertEquals(2, removed.size());
    }

    @Test
    public void testRemoveAllNotPresent() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        Collection<Integer> removed = multimap.removeAll("bar");
        assertSize(2);
        assertNotNull(removed);
        assertTrue(removed.isEmpty());
    }

    @Test
    public void testClear() {
        multimap.put("foo", 1);
        multimap.put("bar", 3);
        multimap.clear();
        assertEquals(0, multimap.keySet().size());
        assertSize(0);
    }

    @Test
    public void testKeySet() {
        multimap.put("foo", 1);
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 3);
        Set<String> keys = multimap.keySet();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("foo"));
        assertTrue(keys.contains(nullKey()));
        assertTrue(keys.containsAll(Lists.newArrayList("foo", nullKey())));
        assertFalse(keys.containsAll(Lists.newArrayList("foo", "bar")));
    }

    @Test
    public void testValues() {
        multimap.put("foo", 1);
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 3);
        Collection<Integer> values = multimap.values();
        assertEquals(3, values.size());
        assertTrue(values.contains(1));
        assertTrue(values.contains(3));
        assertTrue(values.contains(nullValue()));
        assertFalse(values.contains(5));
    }

    @Test
    public void testValuesRemoveAllNullFromEmpty() {
        try {
            multimap.values().removeAll(null);
            // Returning successfully is not ideal, but tolerated.
        }
        catch (NullPointerException expected) {}
    }

    @Test
    public void testValuesRetainAllNullFromEmpty() {
        try {
            multimap.values().retainAll(null);
            // Returning successfully is not ideal, but tolerated.
        }
        catch (NullPointerException expected) {}
    }

    // the entries collection is more thoroughly tested in
    // MultimapCollectionTest
    @SuppressWarnings("unchecked")
    @Test
    public void testEntries() {
        multimap.put("foo", 1);
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 3);
        Collection<Entry<String, Integer>> entries = multimap.entries();
        Set<Entry<String, Integer>> expected = Sets.newHashSet(
                Maps.immutableEntry("foo", 1),
                Maps.immutableEntry("foo", nullValue()),
                Maps.immutableEntry(nullKey(), 3));
        assertEquals(expected, entries);
    }

    @Test
    public void testNoSuchElementException() {
        Iterator<Entry<String, Integer>> entries = multimap.entries()
                .iterator();
        try {
            entries.next();
            fail();
        }
        catch (NoSuchElementException expected) {}
    }

    @Test
    public void testAsMapEntries() {
        multimap.put("foo", 1);
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 3);
        Collection<Entry<String, Collection<Integer>>> entries = multimap
                .asMap().entrySet();
        assertEquals(2, entries.size());

        assertTrue(entries
                .contains(Maps.immutableEntry("foo", multimap.get("foo"))));
        assertFalse(entries
                .contains(Maps.immutableEntry("bar", multimap.get("foo"))));
        assertFalse(entries.contains(Maps.immutableEntry("bar", null)));
        assertFalse(entries.contains(Maps.immutableEntry("foo", null)));
        assertFalse(entries.contains(Maps.immutableEntry("foo", asList(1, 4))));
        assertFalse(entries.contains(Maps.immutableEntry("foot", "oof")));

        Iterator<Entry<String, Collection<Integer>>> iterator = entries
                .iterator();
        for (int i = 0; i < 2; i++) {
            assertTrue(iterator.hasNext());
            Entry<String, Collection<Integer>> entry = iterator.next();
            if("foo".equals(entry.getKey())) {
                assertEquals(2, entry.getValue().size());
                assertTrue(entry.getValue().contains(1));
                assertTrue(entry.getValue().contains(nullValue()));
            }
            else {
                assertEquals(nullKey(), entry.getKey());
                assertEquals(1, entry.getValue().size());
                assertTrue(entry.getValue().contains(3));
            }
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAsMapToString() {
        multimap.put("foo", 1);
        assertEquals("{foo=[1]}", multimap.asMap().toString());
    }

    @Test
    public void testKeysContainsAll() {
        multimap.put("foo", 1);
        multimap.put("foo", 5);
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 3);
        Multiset<String> multiset = multimap.keys();

        assertTrue(multiset.containsAll(asList("foo", nullKey())));
        assertFalse(multiset.containsAll(asList("foo", "bar")));
    }

    @Test
    public void testKeysToString() {
        multimap.put("foo", 7);
        multimap.put("foo", 8);
        assertEquals("[foo x 2]", multimap.keys().toString());
    }

    @Test
    public void testKeysEntrySetToString() {
        multimap.put("foo", 7);
        multimap.put("foo", 8);
        assertEquals("[foo x 2]", multimap.keys().entrySet().toString());
    }

    @Test
    public void testEqualsTrue() {
        multimap.put("foo", 1);
        multimap.put("foo", nullValue());
        multimap.put(nullKey(), 3);
        assertEquals(multimap, multimap);

        Multimap<String, Integer> multimap2 = create();
        multimap2.put(nullKey(), 3);
        multimap2.put("foo", 1);
        multimap2.put("foo", nullValue());

        assertEquals(multimap, multimap2);
        assertEquals(multimap.hashCode(), multimap2.hashCode());
    }

    @Test
    public void testEqualsFalse() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        multimap.put("bar", 3);

        Multimap<String, Integer> multimap2 = create();
        multimap2.put("bar", 3);
        multimap2.put("bar", 1);
        assertFalse(multimap.equals(multimap2));

        multimap2.put("foo", 3);
        assertFalse(multimap.equals(multimap2));

        assertFalse(multimap.equals(nullValue()));
        assertFalse(multimap.equals("foo"));
    }

    @Test
    public void testValuesIterator() {
        multimap.put("foo", 1);
        multimap.put("foo", 2);
        multimap.put(nullKey(), 4);
        int sum = 0;
        for (int i : multimap.values()) {
            sum += i;
        }
        assertEquals(7, sum);
    }

    @Test
    public void testValuesIteratorEmpty() {
        int sum = 0;
        for (int i : multimap.values()) {
            sum += i;
        }
        assertEquals(0, sum);
    }

    @Test
    public void testGetAddQuery() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        multimap.put("bar", 4);
        Collection<Integer> values = multimap.get("foo");
        multimap.put("foo", 5);
        multimap.put("bar", 6);

        /* Verify that values includes effect of put. */
        assertEquals(3, values.size());
        assertTrue(values.contains(1));
        assertTrue(values.contains(5));
        assertFalse(values.contains(6));
        assertEquals(values, Sets.newHashSet(1, 3, 5));
        assertTrue(values.containsAll(asList(3, 5)));
        assertFalse(values.isEmpty());
        assertEquals(multimap.get("foo"), values);
        assertEquals(multimap.get("foo").hashCode(), values.hashCode());
        assertEquals(multimap.get("foo").toString(), values.toString());
    }

    @Test
    public void testGetIterator() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        multimap.put("foo", 5);
        multimap.put("bar", 4);
        Collection<Integer> values = multimap.get("foo");

        Iterator<Integer> iterator = values.iterator();
        assertTrue(iterator.hasNext());
        Integer v1 = iterator.next();
        assertTrue(iterator.hasNext());
        Integer v2 = iterator.next();
        assertTrue(iterator.hasNext());
        Integer v3 = iterator.next();
        assertFalse(iterator.hasNext());

        assertEquals(Sets.newHashSet(v1, v2, v3), Sets.newHashSet(1, 3, 5));
        assertSize(4);
    }

    @Test
    public void testGetRemove() {
        multimap.put("foo", 1);
        multimap.put("foo", 3);
        Collection<Integer> values = multimap.get("foo");
        multimap.remove("foo", 1);
        assertEquals(values, Sets.newHashSet(3));
    }

    @Test
    public void testEntriesRemoveAllNullFromEmpty() {
        try {
            multimap.entries().removeAll(null);
            // Returning successfully is not ideal, but tolerated.
        }
        catch (NullPointerException expected) {}
    }

    @Test
    public void testEntriesRetainAllNullFromEmpty() {
        try {
            multimap.entries().retainAll(null);
            // Returning successfully is not ideal, but tolerated.
        }
        catch (NullPointerException expected) {}
    }

    @Test
    public void testEntriesToString() {
        multimap.put("foo", 3);
        Collection<Entry<String, Integer>> entries = multimap.entries();
        assertEquals("[foo=3]", entries.toString());
    }

    @Test
    public void testEntriesToArray() {
        multimap.put("foo", 3);
        Collection<Entry<String, Integer>> entries = multimap.entries();
        Entry<?, ?>[] array = new Entry<?, ?>[3];
        assertSame(array, entries.toArray(array));
        assertEquals(Maps.immutableEntry("foo", 3), array[0]);
        assertNull(array[1]);
    }

    /**
     * Test calling setValue() on an entry returned by multimap.entries().
     */
    @Test
    public void testEntrySetValue() {
        multimap.put("foo", 1);
        multimap.put("bar", 1);
        Collection<Entry<String, Integer>> entries = multimap.entries();
        Iterator<Entry<String, Integer>> iterator = entries.iterator();
        Entry<String, Integer> entrya = iterator.next();
        Entry<String, Integer> entryb = iterator.next();
        try {
            entrya.setValue(3);
            fail();
        }
        catch (UnsupportedOperationException expected) {}
        assertTrue(multimap.containsEntry("foo", 1));
        assertTrue(multimap.containsEntry("bar", 1));
        assertFalse(multimap.containsEntry("foo", 2));
        assertFalse(multimap.containsEntry("bar", 2));
        assertEquals(1, (int) entrya.getValue());
        assertEquals(1, (int) entryb.getValue());
    }

    /** Verify that the entries remain valid after iterating past them. */
    @Test
    public void testEntriesCopy() {
        multimap.put("foo", 1);
        multimap.put("foo", 2);
        multimap.put("bar", 3);

        Set<Entry<String, Integer>> copy = Sets.newHashSet(multimap.entries());
        assertEquals(3, copy.size());
        assertTrue(copy.contains(Maps.immutableEntry("foo", 1)));
        assertTrue(copy.contains(Maps.immutableEntry("foo", 2)));
        assertTrue(copy.contains(Maps.immutableEntry("bar", 3)));
        assertFalse(copy.contains(Maps.immutableEntry("bar", 1)));

        multimap.removeAll("foo");
        assertEquals(3, copy.size());
        assertTrue(copy.contains(Maps.immutableEntry("foo", 1)));
        assertTrue(copy.contains(Maps.immutableEntry("foo", 2)));
        assertTrue(copy.contains(Maps.immutableEntry("bar", 3)));
        assertFalse(copy.contains(Maps.immutableEntry("bar", 1)));
    }

    @Test
    public void testKeySetRemoveAllNullFromEmpty() {
        try {
            multimap.keySet().removeAll(null);
            fail();
        }
        catch (NullPointerException expected) {}
    }

    @Test
    public void testKeySetRetainAllNullFromEmpty() {
        try {
            multimap.keySet().retainAll(null);
            // Returning successfully is not ideal, but tolerated.
        }
        catch (NullPointerException expected) {}
    }

    @Test
    public void testToStringNull() {
        multimap.put("foo", 3);
        multimap.put("foo", -1);
        multimap.put(nullKey(), nullValue());
        multimap.put("bar", 1);
        multimap.put("foo", 2);
        multimap.put(nullKey(), 0);
        multimap.put("bar", 2);
        multimap.put("bar", nullValue());
        multimap.put("foo", nullValue());
        multimap.put("foo", 4);
        multimap.put(nullKey(), -1);
        multimap.put("bar", 3);
        multimap.put("bar", 1);
        multimap.put("foo", 1);

        // This test is brittle. The original test was meant to validate the
        // contents of the string itself, but key and value ordering tend
        // to change under unpredictable circumstances. Instead, we're just
        // ensuring
        // that the string not return null and, implicitly, not throw an
        // exception.
        assertNotNull(multimap.toString());
    }

    @Test
    public void testEmptyToString() {
        Multimap<String, Integer> map = create();
        assertEquals("{}", map.toString());
        assertEquals("[]", map.entries().toString());
    }

    @Test
    public void testEmptyGetToString() {
        Multimap<String, Integer> map = create();
        map.get("foo"); // shouldn't have any side-effect
        assertEquals("{}", map.toString());
        assertEquals("[]", map.entries().toString());
    }

    @Test
    public void testRemoveToString() {
        Multimap<String, Integer> map = create();
        map.put("foo", 1);
        map.put("foo", 2);
        map.remove("foo", 1);
        assertEquals("[foo=2]", map.entries().toString());
    }
}
