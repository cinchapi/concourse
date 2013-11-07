/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.storage;

import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;

/**
 * 
 * 
 * @author jnelson
 */
public class PrimaryRecordTest extends RecordTest<PrimaryKey, Text, Value> {

	@Override
	protected Text getKey() {
		return TestData.getText();
	}

	@Override
	protected PrimaryKey getLocator() {
		return TestData.getPrimaryKey();
	}

	@Override
	protected PrimaryRecord getRecord(PrimaryKey locator) {
		return Record.createPrimaryRecord(locator);
	}

	@Override
	protected PrimaryRecord getRecord(PrimaryKey locator, Text key) {
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
