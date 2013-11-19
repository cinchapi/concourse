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
import java.util.Set;

import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;

/**
 * Base unit tests for {@link Limbo} services.
 * 
 * @author jnelson
 */
public abstract class LimboTest extends StoreTest {

	@Test
	public void testIterator() {
		List<Write> writes = getWrites();
		for (Write write : writes) {
			add(write.getKey().toString(), write.getValue().getTObject(), write
					.getRecord().longValue());
		}
		Iterator<Write> it0 = ((Limbo) store).iterator();
		Iterator<Write> it1 = writes.iterator();
		while (it1.hasNext()) {
			Assert.assertTrue(it0.hasNext());
			Write w0 = it0.next();
			Write w1 = it1.next();
			Assert.assertEquals(w0, w1);
		}
		Assert.assertFalse(it0.hasNext());
	}

	@Test
	public void testReverseIterator() {
		List<Write> writes = getWrites();
		for (Write write : writes) {
			add(write.getKey().toString(), write.getValue().getTObject(), write
					.getRecord().longValue());
		}
		Iterator<Write> it = ((Limbo) store).reverseIterator();
		int index = writes.size() - 1;
		while (it.hasNext()) {
			Write w = it.next();
			Assert.assertEquals(w, writes.get(index));
			index--;
		}
		Assert.assertEquals(-1, index);
	}

	@Test
	public void testGetVersionForRecord() {
		List<Long> recordPool = getRecordPool();
		long record = recordPool.get(0);
		long version = 0;
		for (int i = 0; i < recordPool.size()
				* ((TestData.getScaleCount() % 3) + 1); i++) {
			int index = TestData.getScaleCount() % recordPool.size();
			long r = recordPool.get(index);
			Write write = Write.add(TestData.getString(),
					Convert.javaToThrift(i), r);
			if(write.getRecord().longValue() == record) {
				version = write.getVersion();
			}
			((Limbo) store).insert(write);
		}
		Assert.assertEquals(version, ((Limbo) store).getVersion(record));
	}

	@Test
	public void testGetVersionForKeyInRecord() {
		List<Long> recordPool = getRecordPool();
		List<String> keyPool = getKeyPool();
		long record = recordPool.get(0);
		String key = keyPool.get(0);
		long version = 0;
		for (int i = 0; i < TestData.getScaleCount(); i++) {
			int index = TestData.getScaleCount()
					% Math.min(recordPool.size(), keyPool.size());
			long r = recordPool.get(index);
			String k = keyPool.get(index);
			Write write = Write.add(k, Convert.javaToThrift(i), r);
			if(write.getRecord().longValue() == record
					&& write.getKey().toString().equals(key)) {
				version = write.getVersion();
			}
			((Limbo) store).insert(write);
		}
		Assert.assertEquals(version, ((Limbo) store).getVersion(key, record));
	}

	private List<Long> getRecordPool() {
		Set<Long> set = Sets.newHashSet();
		for (int i = 0; i < TestData.getScaleCount(); i++) {
			set.add(TestData.getLong());
		}
		return Lists.newArrayList(set);
	}

	private List<String> getKeyPool() {
		Set<String> set = Sets.newHashSet();
		for (int i = 0; i < TestData.getScaleCount(); i++) {
			set.add(TestData.getString());
		}
		return Lists.newArrayList(set);
	}

	@Override
	protected abstract Limbo getStore();

	protected List<Write> getWrites() {
		List<Write> writes = Lists.newArrayList();
		for (int i = 0; i < (TestData.getScaleCount() * 50); i++) {
			writes.add(TestData.getWriteNotStorable());
		}
		return writes;
	}

	@Override
	protected void add(String key, TObject value, long record) {
		((Limbo) store).insert(Write.add(key, value, record));
		
	}

	@Override
	protected void remove(String key, TObject value, long record) {
		((Limbo) store).insert(Write.remove(key, value, record));
		
	}
	
	

}
