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
package org.cinchapi.concourse.server.model.legacy;

import org.cinchapi.concourse.server.io.ByteableTest;
import org.cinchapi.concourse.server.model.legacy.Write;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Write}.
 * 
 * @author jnelson
 */
public class WriteTest extends ByteableTest {

	@Test
	public void testMatchesSameType() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		Assert.assertTrue(Write.add(key, value, record).matches(
				Write.add(key, value, record)));
		Assert.assertTrue(Write.remove(key, value, record).matches(
				Write.remove(key, value, record)));
		Assert.assertTrue(Write.notForStorage(key, value, record).matches(
				Write.notForStorage(key, value, record)));
	}
	
	@Test
	public void testMatchesDiffType(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		Assert.assertFalse(Write.add(key, value, record).matches(
				Write.remove(key, value, record)));
		Assert.assertFalse(Write.add(key, value, record).matches(
				Write.notForStorage(key, value, record)));
		Assert.assertFalse(Write.remove(key, value, record).matches(
				Write.notForStorage(key, value, record)));
	}
	
	@Test
	public void testEqualsDiffType(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		Assert.assertEquals(Write.add(key, value, record), Write.remove(key, value, record));
		Assert.assertEquals(Write.add(key, value, record), Write.notForStorage(key, value, record));
		Assert.assertEquals(Write.remove(key, value, record), Write.notForStorage(key, value, record));
	}
	
	@Test
	public void testEqualsSameType(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		Assert.assertEquals(Write.add(key, value, record), Write.add(key, value, record));
		Assert.assertEquals(Write.remove(key, value, record), Write.remove(key, value, record));
		Assert.assertEquals(Write.notForStorage(key, value, record), Write.notForStorage(key, value, record));
	}

	@Override
	protected Write getRandomTestInstance() {
		return TestData.getWriteAddLegacy();
	}

	@Override
	protected Class<Write> getTestClass() {
		return Write.class;
	}

}
