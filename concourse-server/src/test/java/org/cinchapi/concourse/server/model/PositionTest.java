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

/**
 * Tests for {@link Position}.
 * 
 * @author jnelson
 */
public class PositionTest extends StorableTest {

	@Override
	protected Position getRandomTestInstance() {
		return TestData.getPosition();
	}

	@Override
	protected Class<Position> getTestClass() {
		return Position.class;
	}

	@Override
	protected Position[] getForStorageAndNotForStorageVersionOfObject() {
		long key = TestData.getLong();
		int index = Math.abs(TestData.getInt());
		Position[] array = {
				Position.fromPrimaryKeyAndIndex(PrimaryKey.forStorage(key),
						index),
				Position.fromPrimaryKeyAndIndex(
						PrimaryKey.notForStorage(key), index) };
		return array;
	}

	@Override
	protected Position getForStorageInstance() {
		return TestData.getPosition();
	}

	@Override
	protected Position getNotForStorageInstance() {
		return Position.fromPrimaryKeyAndIndex(
				TestData.getPrimaryKeyNotForStorage(),
				Math.abs(TestData.getInt()));
	}

}
