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
import org.cinchapi.common.io.ByteableTest;
import org.cinchapi.common.io.Byteables;
import org.junit.Test;

import com.google.common.primitives.Longs;

/**
 * Unit tests for the {@link Storabe} interface.
 * 
 * @author jnelson
 */
public abstract class StorableTest extends ByteableTest {

	@Override
	protected final Storable getObject() {
		// This method returns a notForStorage value to be compatible with
		// assumptions made in ByteableTest#copy
		return getNotForStorage();
	}

	@Override
	protected abstract Storable copy(Byteable object);

	/**
	 * Return a forStorage object.
	 * 
	 * @return forStorage
	 */
	protected abstract Storable getForStorage();

	/**
	 * Return a notForStorage object
	 * 
	 * @return notForStorage
	 */
	protected abstract Storable getNotForStorage();

	@Test
	public void testGetForStorageTimestamp() {
		Storable object = getForStorage();
		log.info("Using object '{}'", object);
		Assert.assertTrue((Longs.compare(object.getTimestamp(), Storable.NIL) != 0));
	}

	@Test
	public void testGetNotForStorageTimestamp() {
		Storable object = getNotForStorage();
		log.info("Using object '{}'", object);
		Assert.assertTrue((Longs.compare(object.getTimestamp(), Storable.NIL) == 0));
	}

	@Test
	public void testIsForStorage() {
		Storable forStorage = getForStorage();
		Storable notForStorage = getNotForStorage();
		log.info("Using forStorage object '{}'", forStorage);
		log.info("Using notForStorage '{}'", notForStorage);
		Assert.assertTrue(forStorage.isForStorage());
		Assert.assertFalse(notForStorage.isForStorage());
	}

	@Test
	public void testIsNotForStorage() {
		Storable forStorage = getForStorage();
		Storable notForStorage = getNotForStorage();
		log.info("Using forStorage object '{}'", forStorage);
		log.info("Using notForStorage '{}'", notForStorage);
		Assert.assertFalse(forStorage.isNotForStorage());
		Assert.assertTrue(notForStorage.isNotForStorage());
	}

	@Test
	public void testHashCode() {
		Storable object = getForStorage();
		log.info("Using object '{}'", object);
		Storable copy = copy(object);
		Assert.assertTrue(Longs.compare(object.getTimestamp(),
				copy.getTimestamp()) != 0);
		Assert.assertEquals(object.hashCode(), copy.hashCode());
	}

	@Test
	public void testEquals() {
		Storable object = getForStorage();
		log.info("Using object '{}'", object);
		Storable copy = copy(object);
		Assert.assertTrue(Longs.compare(object.getTimestamp(),
				copy.getTimestamp()) != 0);
		Assert.assertEquals(object, copy);
	}

	@Test
	public void testGetTimestampAfterReadingFromBytes() {
		Storable object = getForStorage();
		log.info("Using object '{}'", object);
		Storable copy = Byteables.read(object.getBytes(), object.getClass());
		Assert.assertEquals(object.getTimestamp(), copy.getTimestamp());
	}

}
