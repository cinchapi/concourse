/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.common.util;

import java.util.Map.Entry;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.util.NonBlockingHashMultimap;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.AbstractMultimapTest;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link NonBlockingHashMultimap} data structure.
 * 
 * @author Jeff Nelson
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
