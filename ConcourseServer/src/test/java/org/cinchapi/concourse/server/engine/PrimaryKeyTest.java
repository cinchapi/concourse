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
package org.cinchapi.concourse.server.engine;

import junit.framework.Assert;

import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.util.Tests;
import org.junit.Test;

/**
 * Unit tests for {@link PrimaryKey}.
 * 
 * @author jnelson
 */
public class PrimaryKeyTest extends StorableTest {

	@Override
	protected Storable copy(Byteable object) {
		return PrimaryKey.notForStorage(((PrimaryKey) object).longValue());
	}

	@Override
	protected Storable getForStorage() {
		return PrimaryKey.forStorage(Tests.getLong());
	}

	@Override
	protected Storable getNotForStorage() {
		return PrimaryKey.notForStorage(Tests.getLong());
	}

	@Test
	public void testLongValue() {
		Long value = Tests.getLong();
		PrimaryKey key = PrimaryKey.forStorage(value);
		Assert.assertEquals(value.longValue(), key.longValue());
	}

	@Test
	public void testIntValue() {
		Long value = Tests.getLong();
		PrimaryKey key = PrimaryKey.forStorage(value);
		Assert.assertEquals(value.intValue(), key.intValue());
	}

	@Test
	public void testFloatValue() {
		Long value = Tests.getLong();
		PrimaryKey key = PrimaryKey.forStorage(value);
		Assert.assertEquals(value.floatValue(), key.floatValue());
	}

	@Test
	public void testDoubleValue() {
		Long value = Tests.getLong();
		PrimaryKey key = PrimaryKey.forStorage(value);
		Assert.assertEquals(value.doubleValue(), key.doubleValue());
	}

	@Test
	public void testByteValue() {
		Long value = Tests.getLong();
		PrimaryKey key = PrimaryKey.forStorage(value);
		Assert.assertEquals(value.byteValue(), key.byteValue());
	}

	@Test
	public void testCompareToLogically() {
		Long value = Math.abs(Tests.getLong());
		Long value2 = value + 1;
		Assert.assertTrue(PrimaryKey.forStorage(value).compareTo(
				PrimaryKey.forStorage(value2)) > 0);

	}

}
