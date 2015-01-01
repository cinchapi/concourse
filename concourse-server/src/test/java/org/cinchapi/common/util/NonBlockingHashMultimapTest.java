/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.common.util;

import java.util.Map.Entry;
import java.util.Set;

import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.AbstractMultimapTest;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link NonBlockingHashMultimap} data structure.
 * 
 * @author jnelson
 */
public class NonBlockingHashMultimapTest extends AbstractMultimapTest {

    @Override
    protected String nullKey() {
        return "null";
    }

    @Override
    protected Integer nullValue() {
        return Integer.MIN_VALUE;
    }

    private NonBlockingHashMultimap<String, Object> map;

    @Override
    protected void afterEachTest() {
        map = null;
    }

    @Override
    protected void beforeEachTest() {
        map = NonBlockingHashMultimap.create();
        super.beforeEachTest();
    }

    @Test
    public void testInsertMultipleValuesForSameKey() {
        String key = TestData.getString();
        Object a = TestData.getObject();
        Object b = null;
        while (b == null || a.equals(b)) {
            b = TestData.getObject();
        }
        map.put(key, a);
        map.put(key, b);
        Assert.assertEquals(Sets.newHashSet(a, b), map.get(key));
    }

    @Test
    public void testSize() {
        int count = TestData.getScaleCount();
        String[] keys = new String[count];
        for (int i = 0; i < count; i++) {
            keys[i] = TestData.getString();
        }
        int totalSize = 0;
        for (int i = 0; i < count * TestData.getScaleCount(); i++) {
            String key = keys[(TestData.getScaleCount() * i) % count];
            Object value = null;
            while (value == null || map.containsEntry(key, value)) {
                value = TestData.getObject();
            }
            map.put(key, value);
            totalSize++;
        }
        Assert.assertEquals(totalSize, map.size());
    }

    @Test
    public void testSizeAfterPutAllIterable() {
        int count = TestData.getScaleCount();
        String[] keys = new String[count];
        for (int i = 0; i < count; i++) {
            keys[i] = TestData.getString();
        }
        int totalSize = 0;
        for (int i = 0; i < count * TestData.getScaleCount(); i++) {
            String key = keys[(TestData.getScaleCount() * i) % count];
            Object value = null;
            while (value == null || map.containsEntry(key, value)) {
                value = TestData.getObject();
            }
            map.put(key, value);
            totalSize++;
        }
        Set<Object> putAll = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            putAll.add(TestData.getObject());
        }
        String key = null;
        while (key == null || map.containsKey(key)) {
            key = TestData.getString();
        }
        map.putAll(key, putAll);
        Assert.assertEquals(totalSize + putAll.size(), map.size());
    }

    @Test
    public void testSizeAfterPutAllMultimap() {
        populate(map);
        NonBlockingHashMultimap<String, Object> other = NonBlockingHashMultimap
                .create();
        populate(other);
        int expected = Sets.union((Set<Entry<String, Object>>) map.entries(),
                (Set<Entry<String, Object>>) other.entries()).size(); // account
                                                                      // for any
                                                                      // duplicates
        map.putAll(other);
        Assert.assertEquals(expected, map.size());

    }

    @Test
    public void testSizeAfterRemoves() {
        // TODO
    }

    /**
     * Populate {@code map} with random data.
     */
    private void populate(NonBlockingHashMultimap<String, Object> map) {
        int count = TestData.getScaleCount();
        String[] keys = new String[count];
        for (int i = 0; i < count; i++) {
            keys[i] = TestData.getString();
        }
        for (int i = 0; i < count * TestData.getScaleCount(); i++) {
            String key = keys[(TestData.getScaleCount() * i) % count];
            Object value = null;
            while (value == null || map.containsEntry(key, value)) {
                value = TestData.getObject();
            }
            map.put(key, value);
        }
    }

    @Override
    protected Multimap<String, Integer> create() {
        return NonBlockingHashMultimap.create();
    }

}
