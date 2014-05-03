/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage.db;

import java.util.Set;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Unit tests for {@link BrowsableRecord}.
 * 
 * @author jnelson
 */
public abstract class BrowsableRecordTest<L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>>
        extends RecordTest<L, K, V> {

    @Test
    public void testBrowse() {
        Multimap<K, V> expected = HashMultimap.create();
        L locator = getLocator();
        record = getRecord(locator);
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            K key = getKey();
            Set<V> values = populateRecord(record, locator, key);
            expected.putAll(key, values);
        }
        Assert.assertEquals(expected.asMap(),
                ((BrowsableRecord<L, K, V>) record).browse());
    }

    @Test
    public void testBrowseWithTime() {
        Multimap<K, V> expected = HashMultimap.create();
        L locator = getLocator();
        record = getRecord(locator);
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            K key = getKey();
            Set<V> values = populateRecord(record, locator, key);
            expected.putAll(key, values);
        }
        long timestamp = Time.now();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            K key = getKey();
            populateRecord(record, locator, key);
        }
        Assert.assertEquals(expected.asMap(),
                ((BrowsableRecord<L, K, V>) record).browse(timestamp));
    }

}
