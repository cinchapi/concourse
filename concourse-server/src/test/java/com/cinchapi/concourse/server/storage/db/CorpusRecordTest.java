/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import org.junit.Test;

import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link SearchIndex}.
 * 
 * @author Jeff Nelson
 */
public class CorpusRecordTest extends RecordTest<Text, Text, Position> {

    @Override
    protected CorpusRecord getRecord(Text locator) {
        return CorpusRecord.create(locator);
    }

    @Override
    protected CorpusRecord getRecord(Text locator, Text key) {
        return CorpusRecord.createPartial(locator, key);
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
    protected CorpusRevision getRevision(Text locator, Text key,
            Position value) {
        return Revision.createCorpusRevision(locator, key, value, Time.now(),
                getAction(locator, key, value));
    }

    @Override
    @Test
    public void testBrowseWithTime() {/* historical reads are not supported */}

    @Override
    @Test
    public void testGetWithTime() {/* historical reads are not supported */}

    @Override
    @Test
    public void testDescribeWithTime() {/* historical reads are not supported */}

}
