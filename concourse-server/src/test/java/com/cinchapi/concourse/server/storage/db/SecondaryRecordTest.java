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

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.SecondaryRecord;
import com.cinchapi.concourse.server.storage.db.SecondaryRevision;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link SecondaryRecord}.
 * 
 * @author Jeff Nelson
 */
public class SecondaryRecordTest extends
        BrowsableRecordTest<Text, Value, PrimaryKey> {

    @Override
    protected Value getKey() {
        return TestData.getValue();
    }

    @Override
    protected Text getLocator() {
        return TestData.getText();
    }

    @Override
    protected SecondaryRecord getRecord(Text locator) {
        return Record.createSecondaryRecord(locator);
    }

    @Override
    protected SecondaryRecord getRecord(Text locator, Value key) {
        return Record.createSecondaryRecordPartial(locator, key);
    }

    @Override
    protected SecondaryRevision getRevision(Text locator, Value key,
            PrimaryKey value) {
        return Revision.createSecondaryRevision(locator, key, value,
                Time.now(), getAction(locator, key, value));
    }

    @Override
    protected PrimaryKey getValue() {
        return TestData.getPrimaryKey();
    }

    @Test
    public void testFindBrowseOnlyReturnsRelevantData() {
        Text locator = TestData.getText();
        record = getRecord(locator);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j <= i; j++) {
                record.append(getRevision(locator,
                        Value.wrap(Convert.javaToThrift(j)), PrimaryKey.wrap(i)));
            }
        }
        Map<PrimaryKey, Set<Value>> data = ((SecondaryRecord) record).explore(
                Operator.GREATER_THAN, Value.wrap(Convert.javaToThrift(50)));
        for (int i = 0; i < 100; i++) {
            PrimaryKey pk = PrimaryKey.wrap(i);
            if(i > 50) {
                Assert.assertTrue(data.containsKey(pk));
                Assert.assertEquals(i - 50, data.get(pk).size());
                for (Value value : data.get(pk)) {
                    Assert.assertTrue(value.compareTo(Value.wrap(Convert
                            .javaToThrift(50))) > 0);
                }
            }
            else {
                Assert.assertFalse(data.containsKey(pk));
            }

        }
    }
}
