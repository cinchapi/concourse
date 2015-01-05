/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.storage.db.Record;
import org.cinchapi.concourse.server.storage.db.Revision;
import org.cinchapi.concourse.server.storage.db.SearchRecord;
import org.cinchapi.concourse.server.storage.db.SearchRevision;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link SearchIndex}.
 * 
 * @author jnelson
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
