/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.PrimaryRecord;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link PrimaryRecord}.
 * 
 * @author Jeff Nelson
 */
public class PrimaryRecordTest extends BrowsableRecordTest<PrimaryKey, Text, Value> {

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
    protected Record<PrimaryKey, Text, Value> getRecord(PrimaryKey locator, Text key) {
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
    public void testChronologize(){       
        Map<Long, Set<TObject>> map = Maps.newLinkedHashMap();
        Set<TObject> set = Sets.newLinkedHashSet();
        long recordId = TestData.getLong();
        PrimaryKey primaryKey = PrimaryKey.wrap(recordId);
        PrimaryRecord record = PrimaryRecord.createPrimaryRecord(primaryKey);        
        for (long i = 30; i <= 50; i++) {
            TObject tObject = Convert.javaToThrift("foo" + i);
            record.append(getRevision(primaryKey, Text.wrapCached("name"), Value.wrap(tObject)));
        }
        long start = Time.now();
        for (long i = 51; i <= 70; i++) {
            set = Sets.newLinkedHashSet(set);
            TObject tObject = Convert.javaToThrift("foo" + i);
            record.append(getRevision(primaryKey, Text.wrapCached("name"), Value.wrap(tObject)));
            set.add(tObject);
            map.put(i, set);
        }
        long end = Time.now();
        for (long i = 71; i <= 90; i++) {
            TObject tObject = Convert.javaToThrift("foo" + i);
            record.append(getRevision(primaryKey, Text.wrapCached("name"), Value.wrap(tObject)));
        }
        Map<Long, Set<TObject>> newMap = record.chronologize(Text.wrapCached("name"), start, end);
        long key = 51;
        for(Entry<Long, Set<TObject>> e : newMap.entrySet()){
            Set<TObject> result = e.getValue();
            Assert.assertEquals(map.get(key), result);
            key++;
        }
    }
}
