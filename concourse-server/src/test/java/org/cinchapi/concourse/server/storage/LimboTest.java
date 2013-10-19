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

import java.util.Iterator;
import java.util.List;

import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Base unit tests for {@link Limbo} services.
 * 
 * @author jnelson
 */
public abstract class LimboTest extends ProxyStoreTest {

	@Test
	public void testIterator() {
		List<Write> writes = getWrites();
		for (Write write : writes) {
			add(write.getKey().toString(), write.getValue().getTObject(), write
					.getRecord().longValue());
		}
		Iterator<Write> it0 = ((Limbo) store).iterator();
		Iterator<Write> it1 = writes.iterator();
		while(it1.hasNext()){
			Assert.assertTrue(it0.hasNext());
			Write w0 = it0.next();
			Write w1 = it1.next();
			Assert.assertEquals(w0, w1);
		}
		Assert.assertFalse(it0.hasNext());
 	}

	@Override
	protected abstract Limbo getStore();

	private List<Write> getWrites() {
		List<Write> writes = Lists.newArrayList();
		for (int i = 0; i < (TestData.getScaleCount() * 50); i++) {
			writes.add(TestData.getWriteNotStorable());
		}
		return writes;
	}

}
