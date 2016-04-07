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

import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.db.PrimaryRecord;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

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

}
