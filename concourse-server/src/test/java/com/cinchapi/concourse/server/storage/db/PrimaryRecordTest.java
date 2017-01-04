/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.PrimaryRecord;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link PrimaryRecord}.
 * 
 * @author Jeff Nelson
 */
public class PrimaryRecordTest extends
        BrowsableRecordTest<PrimaryKey, Text, Value> {

    @Override
    protected Text getKey() {
        return TestData.getText();
    }

    @Override
    protected PrimaryKey getLocator() {
        return TestData.getPrimaryKey();
    }

    @Override
    protected Record<PrimaryKey, Text, Value> getRecord(PrimaryKey locator) {
        return Record.createPrimaryRecord(locator);
    }

    @Override
    protected Record<PrimaryKey, Text, Value> getRecord(PrimaryKey locator,
            Text key) {
        return Record.createPrimaryRecordPartial(locator, key);
    }

    @Override
    protected Revision<PrimaryKey, Text, Value> getRevision(PrimaryKey locator,
            Text key, Value value) {
        return Revision.createPrimaryRevision(locator, key, value, Time.now(),
                getAction(locator, key, value));
    }

    @Override
    protected Value getValue() {
        return TestData.getValue();
    }

    @Test
    public void testChronologize() {
        Map<Long, Set<Value>> map = Maps.newLinkedHashMap();
        Set<Value> set = Sets.newLinkedHashSet();
        Set<Value> allValues = Sets.newLinkedHashSet();
        long recordId = TestData.getLong();
        PrimaryKey primaryKey = PrimaryKey.wrap(recordId);
        PrimaryRecord record = PrimaryRecord.createPrimaryRecord(primaryKey);
        for (long i = 30; i <= 35; i++) {
            Value value = null;
            while (value == null || !allValues.add(value)) {
                value = TestData.getValue();
            }
            record.append(getRevision(primaryKey, Text.wrapCached("name"),
                    value));
            set.add(value);
        }
        long start = Time.now();
        for (long i = 36; i <= 45; i++) {
            set = Sets.newLinkedHashSet(set);
            Value value = null;
            while (value == null || !allValues.add(value)) {
                value = TestData.getValue();
            }
            record.append(getRevision(primaryKey, Text.wrapCached("name"),
                    value));
            set.add(value);
            map.put(i, set);
        }
        long end = Time.now();
        for (long i = 51; i <= 60; i++) {
            Value value = null;
            while (value == null || !allValues.add(value)) {
                value = TestData.getValue();
            }
            record.append(getRevision(primaryKey, Text.wrapCached("name"),
                    value));
        }
        Map<Long, Set<Value>> newMap = record.chronologize(
                Text.wrapCached("name"), start, end);
        long key = 36;
        for (Entry<Long, Set<Value>> e : newMap.entrySet()) {
            Set<Value> result = e.getValue();
            Assert.assertEquals(map.get(key), result);
            key++;
        }
    }
}
