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

import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.db.Record;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.SearchRecord;
import com.cinchapi.concourse.server.storage.db.SearchRevision;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link SearchIndex}.
 * 
 * @author Jeff Nelson
 */
public class SearchRecordTest extends RecordTest<Text, Text, Position> {

    @Override
    protected SearchRecord getRecord(Text locator) {
        return Record.createSearchRecord(locator);
    }

    @Override
    protected SearchRecord getRecord(Text locator, Text key) {
        return Record.createSearchRecordPartial(locator, key);
    }

    @Override
    protected Text getLocator() {
        return TestData.getText();
    }

    @Override
    protected Text getKey() {
        return TestData.getText();
    }

    @Override
    protected Position getValue() {
        return TestData.getPosition();
    }

    @Override
    protected SearchRevision getRevision(Text locator, Text key, Position value) {
        return Revision.createSearchRevision(locator, key, value, Time.now(),
                getAction(locator, key, value));
    }

}
