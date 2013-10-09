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
package org.cinchapi.concourse.server.model;

import org.cinchapi.concourse.server.io.StorableTest;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link PrimaryKey}.
 * 
 * @author jnelson
 */
public class PrimaryKeyTest extends StorableTest {

	@Test
	public void testCompareToLogically() {
		long value1 = TestData.getLong();
		value1 = value1 == Long.MAX_VALUE ? value1 - 1 : value1;
		long value2 = value1 + 1;
		PrimaryKey key1 = PrimaryKey.forStorage(value1);
		PrimaryKey key2 = PrimaryKey.forStorage(value2);
		Assert.assertTrue(key1.compareTo(key2) < 0);
	}

	@Test
	public void testCompareToTemporallyWithTwoForStorage() {
		long value = TestData.getLong();
		PrimaryKey key1 = PrimaryKey.forStorage(value);
		PrimaryKey key2 = PrimaryKey.forStorage(value);
		Assert.assertTrue(key1.compareTo(key2, false) > 0); // smaller timestamp
															// is "greater" and
															// pushed to the
															// back for
															// descending order
															// sorting
	}

	@Test
	public void testCompareToTemporallyWithForStorageAndNotForStorage() {
		long value = TestData.getLong();
		PrimaryKey key1 = PrimaryKey.notForStorage(value);
		PrimaryKey key2 = PrimaryKey.forStorage(value);
		Assert.assertTrue(key1.compareTo(key2, false) > 0); // notForStorage is
															// always pushed to
															// the back
	}
	
	@Test
	public void testSize(){
		PrimaryKey key = TestData.getPrimaryKeyForStorage();
		Assert.assertEquals(PrimaryKey.SIZE, key.size());
		Assert.assertEquals(PrimaryKey.SIZE, key.getBytes().capacity());
	}

	@Override
	protected PrimaryKey[] getForStorageAndNotForStorageVersionOfObject() {
		long key = TestData.getLong();
		return new PrimaryKey[] { PrimaryKey.forStorage(key),
				PrimaryKey.notForStorage(key) };
	}

	@Override
	protected PrimaryKey getForStorageInstance() {
		return TestData.getPrimaryKeyForStorage();
	}

	@Override
	protected PrimaryKey getNotForStorageInstance() {
		return TestData.getPrimaryKeyNotForStorage();
	}

	@Override
	protected Class<PrimaryKey> getTestClass() {
		return PrimaryKey.class;
	}

}
