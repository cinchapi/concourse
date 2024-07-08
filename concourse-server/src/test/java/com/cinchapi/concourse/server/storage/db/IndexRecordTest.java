/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for
 * {@link com.cinchapi.concourse.server.storage.db.IndexRecord}.
 *
 * @author Jeff Nelson
 */
public class IndexRecordTest extends RecordTest<Text, Value, Identifier> {

    @Override
    protected Value getKey() {
        return TestData.getValue();
    }

    @Override
    protected Text getLocator() {
        return TestData.getText();
    }

    @Override
    protected IndexRecord getRecord(Text locator) {
        return IndexRecord.create(locator);
    }

    @Override
    protected IndexRecord getRecord(Text locator, Value key) {
        return IndexRecord.createPartial(locator, key);
    }

    @Override
    protected IndexRevision getRevision(Text locator, Value key,
            Identifier value) {
        return Revision.createIndexRevision(locator, key, value, Time.now(),
                getAction(locator, key, value));
    }

    @Override
    protected Identifier getValue() {
        return TestData.getIdentifier();
    }

    @Test
    public void testFindBrowseOnlyReturnsRelevantData() {
        Text locator = TestData.getText();
        record = getRecord(locator);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j <= i; j++) {
                record.append(getRevision(locator,
                        Value.wrap(Convert.javaToThrift(j)), Identifier.of(i)));
            }
        }
        Map<Identifier, Set<Value>> data = ((IndexRecord) record).findAndGet(
                Operator.GREATER_THAN, Value.wrap(Convert.javaToThrift(50)));
        for (int i = 0; i < 100; i++) {
            Identifier pk = Identifier.of(i);
            if(i > 50) {
                Assert.assertTrue(data.containsKey(pk));
                Assert.assertEquals(i - 50, data.get(pk).size());
                for (Value value : data.get(pk)) {
                    Assert.assertTrue(value.compareTo(
                            Value.wrap(Convert.javaToThrift(50))) > 0);
                }
            }
            else {
                Assert.assertFalse(data.containsKey(pk));
            }
        }
    }

    @Test
    public void testCaseInsensitiveValuesNotLost() {
        Text locator = Text.wrap("major");
        record = getRecord(locator);
        record.append(getRevision(locator,
                Value.wrap(Convert.javaToThrift("Business Management")),
                Identifier.of(1)));
        record.append(getRevision(locator,
                Value.wrap(Convert.javaToThrift("business management")),
                Identifier.of(2)));
        IndexRecord index = (IndexRecord) record;
        Map<Identifier, Set<Value>> data = index.findAndGet(Operator.REGEX,
                Value.wrap(Convert.javaToThrift(".*business.*")));
        Assert.assertFalse(data.isEmpty());
    }

    @Test
    public void testContains() {
        Text locator = Text.wrap("major");
        record = getRecord(locator);
        record.append(getRevision(locator,
                Value.wrap(Convert.javaToThrift("Business Management")),
                Identifier.of(1)));
        record.append(getRevision(locator,
                Value.wrap(Convert.javaToThrift("business management")),
                Identifier.of(2)));
        IndexRecord index = (IndexRecord) record;
        Map<Identifier, Set<Value>> data = index.findAndGet(Operator.CONTAINS,
                Value.wrap(Convert.javaToThrift("business")));
        Assert.assertFalse(data.isEmpty());
    }
}
