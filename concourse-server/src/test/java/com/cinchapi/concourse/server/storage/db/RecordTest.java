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
package com.cinchapi.concourse.server.storage.db;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Record} and its subclasses.
 * 
 * 
 * @author Jeff Nelson
 * @param <L>
 * @param <K>
 * @param <V>
 */
public abstract class RecordTest<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> {

    protected Record<L, K, V> record;

    /**
     * The last actions
     */
    private Map<Composite, Action> actions = Maps.newHashMap();

    /**
     * Get the appropriate action for {@code locator}, {@code key} and
     * {@code value} to maintain the offsetting constraint.
     * 
     * @param locator
     * @param key
     * @param value
     * @return the appropriate action
     */
    protected Action getAction(L locator, K key, V value) {
        Composite comp = Composite.create(locator, key, value);
        Action action = actions.get(comp);
        if(action == null || action == Action.REMOVE) {
            action = Action.ADD;
        }
        else {
            action = Action.REMOVE;
        }
        actions.put(comp, action);
        return action;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannontAppendWrongLocator() {
        L locator1 = getLocator();
        record = getRecord(locator1);
        L locator2 = null;
        while (locator2 == null || locator1.equals(locator2)) {
            locator2 = getLocator();
        }
        record.append(getRevision(locator2, getKey(), getValue()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotAppendSmallerVersion() {
        record = getRecord();
        Revision<L, K, V> r1 = getRevision();
        Revision<L, K, V> r2 = getRevision();
        record.append(r2);
        record.append(r1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotAppendWrongKey() {
        K key1 = getKey();
        record = getRecord(getLocator(), key1);
        K key2 = null;
        while (key2 == null || key1.equals(key2)) {
            key2 = getKey();
        }
        record.append(getRevision(getLocator(), key2, getValue()));
    }

    @Test
    public void testEqualsNotPartial() {
        L locator = getLocator();
        record = getRecord(locator);
        Assert.assertEquals(record, getRecord(locator));
    }

    @Test
    public void testEqualsPartial() {
        L locator = getLocator();
        K key = getKey();
        record = getRecord(locator, key);
        Assert.assertEquals(record, getRecord(locator, key));
    }

    @Test
    public void testNotEqualsNotPartial() {
        L l1 = getLocator();
        L l2 = null;
        while (l2 == null || l1.equals(l2)) {
            l2 = getLocator();
        }
        Assert.assertFalse(getRecord(l1).equals(getRecord(l2)));
    }

    @Test
    public void testNotEqualsPartial() {
        L locator = getLocator();
        K k1 = getKey();
        K k2 = null;
        while (k2 == null || k1.equals(k2)) {
            k2 = getKey();
        }
        Assert.assertFalse(getRecord(locator, k1)
                .equals(getRecord(locator, k2)));
    }

    @Test
    public void testGet() {
        L locator = getLocator();
        K key = getKey();
        record = getRecord(locator, key);
        Set<V> values = populateRecord(record, locator, key);
        Assert.assertEquals(values, record.get(key));
    }
    
    @Test
    public void testDescribe(){
        L locator = getLocator();
        Set<K> keys = Sets.newHashSet();
        record = getRecord(locator);
        for(int i = 0; i < TestData.getScaleCount(); i++){
            K key = getKey();
            keys.add(key);
            populateRecord(record, locator, key);
        }
        Assert.assertEquals(keys, record.describe());
    }
    
    @Test
    public void testDescribeWithTime(){
        L locator = getLocator();
        Set<K> keys = Sets.newHashSet();
        record = getRecord(locator);
        for(int i = 0; i < TestData.getScaleCount(); i++){
            K key = getKey();
            keys.add(key);
            populateRecord(record, locator, key);
        }
        long timestamp = Time.now();
        for(int i = 0; i < TestData.getScaleCount(); i++){
            K key = getKey();
            populateRecord(record, locator, key);
        }
        Assert.assertEquals(keys, record.describe(timestamp));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetWithTime() {
        L locator = getLocator();
        K key = getKey();
        record = getRecord(locator, key);
        Set<V> values = populateRecord(record, locator, key);
        long timestamp = Time.now();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            if(TestData.getInt() % 3 == 0) {
                Object[] array;
                V value = (V) (array = (values.toArray()))[Math.abs(TestData
                        .getInt()) % array.length];
                record.append(getRevision(locator, key, value));
            }
            else if(TestData.getInt() % 5 == 0) {
                V value = null;
                while (value == null || values.contains(value)) {
                    value = getValue();
                }
                record.append(getRevision(locator, key, value));
            }
            else {
                continue;
            }
        }
        Assert.assertEquals(values, record.get(key, timestamp));
    }

    @Test
    public void testIsPartialIfCreatedWithKey() {
        record = getRecord(getLocator(), getKey());
        Assert.assertTrue(record.isPartial());
    }

    @Test
    public void testNotPartialIfNotCreatedWithKey() {
        record = getRecord(getLocator());
        Assert.assertFalse(record.isPartial());
    }

    @Test
    public void testNotPresentAfterEvenNumberAppends() {
        L locator = getLocator();
        K key = getKey();
        V value = getValue();
        record = getRecord(locator, key);
        int count = TestData.getScaleCount();
        count += Numbers.isOdd(count) ? 1 : 0;
        for (int i = 0; i < count; i++) {
            record.append(getRevision(locator, key, value));
        }
        Assert.assertFalse(record.get(key).contains(value));
    }

    @Test
    public void testPresentAfterOddNumberAppends() {
        L locator = getLocator();
        K key = getKey();
        V value = getValue();
        record = getRecord(locator, key);
        int count = TestData.getScaleCount();
        count += Numbers.isEven(count) ? 1 : 0;
        for (int i = 0; i < count; i++) {
            record.append(getRevision(locator, key, value));
        }
        Assert.assertTrue(record.get(key).contains(value));
    }
    
    @Test
    public void testIsEmpty(){
        L locator = getLocator();
        K key = getKey();
        V value = getValue();
        record = getRecord(locator, key);
        Assert.assertTrue(record.isEmpty());
        record.append(getRevision(locator, key, value));
        Assert.assertFalse(record.isEmpty());
        record.append(getRevision(locator, key, value));
        Assert.assertTrue(record.describe().isEmpty());
        Assert.assertFalse(record.isEmpty());
    }

    protected abstract K getKey();

    protected abstract L getLocator();

    protected abstract Record<L, K, V> getRecord(L locator);

    protected abstract Record<L, K, V> getRecord(L locator, K key);

    protected abstract Revision<L, K, V> getRevision(L locator, K key, V value);

    protected abstract V getValue();

    private Record<L, K, V> getRecord() {
        return getRecord(getLocator());
    }

    private Revision<L, K, V> getRevision() {
        return getRevision(getLocator(), getKey(), getValue());
    }

    protected Set<V> populateRecord(Record<L, K, V> record, L locator, K key) {
        Set<V> values = Sets.newHashSet();
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            V value = null;
            while (value == null || values.contains(value)) {
                value = getValue();
            }
            record.append(getRevision(locator, key, value));
            values.add(value);
        }
        return values;
    }

}
