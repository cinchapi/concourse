/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

import java.util.Map;
import java.util.Set;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.io.Composite;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Record} and its subclasses.
 * 
 * 
 * @author jnelson
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

    private Set<V> populateRecord(Record<L, K, V> record, L locator, K key) {
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
