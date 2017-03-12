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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.storage.db.BrowsableRecord;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Unit tests for {@link BrowsableRecord}.
 * 
 * @author Jeff Nelson
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
        L locator = Variables.register("locator", getLocator());
        record = Variables.register("record", getRecord(locator));
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            K key = Variables.register("key", getKey());
            Set<V> values = populateRecord(record, locator, key);
            Variables.register("values", values);
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
