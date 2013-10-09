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
package org.cinchapi.concourse.server.io;

import org.cinchapi.concourse.server.model.Storable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link Storable} objects.
 * 
 * @author jnelson
 */
public abstract class StorableTest extends ByteableTest {

	@Test
	public void testForStorageAndNotForStorageEquality() {
		Storable[] instances = getForStorageAndNotForStorageVersionOfObject();
		Assert.assertEquals(instances[0], instances[1]);

	}

	@Test
	public void testForStorageAndNotForStorageSameHashcode() {
		Storable[] instances = getForStorageAndNotForStorageVersionOfObject();
		Assert.assertEquals(instances[0].hashCode(), instances[1].hashCode());
	}

	@Test
	public void testForStorageHasTimestamp() {
		Assert.assertFalse(Long.compare(Storable.NO_TIMESTAMP,
				getForStorageInstance().getTimestamp()) == 0);
	}

	@Test
	public void testNotForStorageHasNoTimestamp() {
		Assert.assertEquals(Storable.NO_TIMESTAMP, getNotForStorageInstance()
				.getTimestamp());
	}

	/**
	 * Return a forStorage and notForStorage version of the same logical
	 * instance.
	 * 
	 * @return an array containing a forStorage and notForStorage instance
	 */
	protected abstract Storable[] getForStorageAndNotForStorageVersionOfObject();

	/**
	 * Return a forStorage instance.
	 * 
	 * @return a forStorage instance
	 */
	protected abstract Storable getForStorageInstance();

	/**
	 * Return a notForStorage instance.
	 * 
	 * @return a notForStorage instance
	 */
	protected abstract Storable getNotForStorageInstance();


	@Override
	protected final Byteable getRandomTestInstance() {
		return getForStorageInstance();
	}
	
	

}
