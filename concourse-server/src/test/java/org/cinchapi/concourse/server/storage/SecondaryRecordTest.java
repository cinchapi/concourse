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
public class SecondaryRecordTest extends RecordTest<Text, Value, PrimaryKey>{

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
	protected SecondaryRevision getRevision(Text locator,
			Value key, PrimaryKey value) {
		return Revision.createSecondaryRevision(locator, key, value, Time.now());
	}

	@Override
	protected PrimaryKey getValue() {
		return TestData.getPrimaryKey();
	}

}
