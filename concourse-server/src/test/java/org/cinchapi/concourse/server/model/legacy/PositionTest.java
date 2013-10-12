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

import org.cinchapi.concourse.server.io.StorableTest;
import org.cinchapi.concourse.server.model.legacy.PrimaryKey;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link Position}.
 * 
 * @author jnelson
 */
public class PositionTest extends StorableTest {

	@Test
	public void testCompareToSamePrimaryKeyAndSameIndex() {
		PrimaryKey key = TestData.getPrimaryKeyForStorage();
		int index = Math.abs(TestData.getInt());
		Position p1 = Position.fromPrimaryKeyAndIndex(key, index);
		Position p2 = Position.fromPrimaryKeyAndIndex(key, index);
		Assert.assertTrue(p1.compareTo(p2) == 0);
	}

	@Test
	public void testCompareToSamePrimaryKeyAndDiffIndex() {
		PrimaryKey key = TestData.getPrimaryKeyForStorage();
		int index1 = Math.abs(TestData.getInt());
		index1 = index1 == Integer.MAX_VALUE ? index1 - 1 : index1;
		int index2 = index1 + 1;
		Position p1 = Position.fromPrimaryKeyAndIndex(key, index1);
		Position p2 = Position.fromPrimaryKeyAndIndex(key, index2);
		Assert.assertTrue(p1.compareTo(p2) < 0);
	}

	@Test
	public void testCompareToDiffPrimaryKey() {
		long long1 = TestData.getLong();
		long1 = long1 == Long.MAX_VALUE ? long1 - 1 : long1;
		long long2 = long1 + 1;
		PrimaryKey key1 = PrimaryKey.forStorage(long1);
		PrimaryKey key2 = PrimaryKey.forStorage(long2);
		Position p1 = Position.fromPrimaryKeyAndIndex(key1,
				Math.abs(TestData.getInt()));
		Position p2 = Position.fromPrimaryKeyAndIndex(key2,
				Math.abs(TestData.getInt()));
		Assert.assertTrue(p1.compareTo(p2) < 0);
	}

	@Test
	public void testSize() {
		Position p = TestData.getPositionLegacy();
		Assert.assertEquals(Position.SIZE, p.size());
		Assert.assertEquals(Position.SIZE, p.getBytes().capacity());
	}

	@Override
	protected Class<Position> getTestClass() {
		return Position.class;
	}

	@Override
	protected Position[] getForStorageAndNotForStorageVersionOfObject() {
		long key = TestData.getLong();
		int index = Math.abs(TestData.getInt());
		return new Position[] {
				Position.fromPrimaryKeyAndIndex(PrimaryKey.forStorage(key),
						index),
				Position.fromPrimaryKeyAndIndex(PrimaryKey.notForStorage(key),
						index) };
	}

	@Override
	protected Position getForStorageInstance() {
		return TestData.getPositionLegacy();
	}

	@Override
	protected Position getNotForStorageInstance() {
		return Position.fromPrimaryKeyAndIndex(
				TestData.getPrimaryKeyNotForStorage(),
				Math.abs(TestData.getInt()));
	}

}
