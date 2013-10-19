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

import org.cinchapi.concourse.server.model.Position;
import org.cinchapi.concourse.server.model.PrimaryKey;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * 
 * @author jnelson
 */
public class SearchBlockTest extends BlockTest<Text, Text, Position>{
	
	

	@Override
	@Test(expected = IllegalStateException.class)
	public void testCannotInsertInImmutableBlock() {
		((SearchBlock) block).insert(getLocator(), getStringValue(), getRecord(), Time.now());
		block.sync();
		((SearchBlock) block).insert(getLocator(), getStringValue(), getRecord(), Time.now());
	}
	
	private Value getStringValue(){
		return Value.wrap(Convert.javaToThrift(TestData.getString()));
	}
	
	private PrimaryKey getRecord(){
		return TestData.getPrimaryKey();
	}

	@Override
	@Test
	public void testMightContainLocatorKeyValue() {
		Text locator = getLocator();
		Value value = Value.wrap(Convert.javaToThrift(TestData.getString()));
		Text term = Text.wrap(value.getObject().toString().split(" ")[0].trim());
		PrimaryKey record = getRecord();
		Assert.assertFalse(block.mightContain(locator, term, Position.wrap(record, 0)));
		((SearchBlock) block).insert(locator, value, record, Time.now());
		Assert.assertTrue(block.mightContain(locator, term, Position.wrap(record, 0)));
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
	protected SearchBlock getMutableBlock(String directory) {
		return Block.createSearchBlock(Long.toString(Time.now()), directory);
	}

}
