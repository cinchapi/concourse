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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.io.ByteableTest;
import org.cinchapi.common.util.Tests;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base unit tests for {@link Record} implementations.
 * 
 * @author jnelson
 */
public abstract class RecordTest<L extends Byteable, K extends Byteable, V extends Storable>
		extends ByteableTest {

	private Record<L, K, V> record;

	@After
	public void tearDown() {
		record.delete(); //authorized
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.cinchapi.common.io.ByteableTest#getObject()
	 */
	@Override
	protected Byteable getObject() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cinchapi.common.io.ByteableTest#copy(org.cinchapi.common.io.Byteable)
	 */
	@Override
	protected Byteable copy(Byteable object) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Record<L, K, V> newInstance() {
		return newInstance(getLocator());
	}

	protected abstract Record<L, K, V> newInstance(L locator);

	protected abstract L getLocator();

	protected abstract K getKey();

	protected abstract V getForStorageValue();
	
	protected abstract V getForStorageValueCopy(V value);

	protected Map<K, V> populateWithAdd(Record<L, K, V> record) {
		Map<K, V> writes = Maps.newHashMap();
		int count = Tests.getScaleCount();
		for (int i = 0; i < count; i++) {
			K key = getKey();
			V value = getForStorageValue();
			while (record.get(key).contains(value)) {
				value = getForStorageValue();
			}
			writes.put(key, value);
			record.add(key, value);
		}
		return writes;
	}

	protected Map<K, V> populateWithAddAndRemove(Record<L, K, V> record) {
		Map<K, V> writes = populateWithAdd(record);
		Iterator<Entry<K, V>> it = writes.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, V> entry = it.next();
			if(Tests.getInt() % 2 == 0) {
				record.remove(entry.getKey(), getForStorageValueCopy(entry.getValue()));
				it.remove();
			}
		}
		return writes;
	}

	@Test
	public void testFsync() {
		L locator = getLocator();
		record = newInstance(locator);
		populateWithAddAndRemove(record);
		record.fsync();
		Record<L,K,V> copy = newInstance(locator);
		try{
			Assert.assertEquals(record, copy);
		}
		finally{
			copy.delete();
		}

	}

	@Test
	public void testAdd() {
		record = newInstance();
		K key = getKey();
		V value = getForStorageValue();
		record.add(key, value);
		Assert.assertTrue(record.get(key).contains(value));
	}
	
	@Test
	public void testAddMultipleValuesForKey(){
		record = newInstance();
		K key = getKey();
		List<V> values = Lists.newArrayList();
		int count = Tests.getScaleCount();
		for(int i = 0; i < count; i++){
			V value = getForStorageValue();
			while(record.get(key).contains(value)){
				value = getForStorageValue();
			}
			record.add(key, value);
			values.add(value);
		}
		for(V value : values){
			Assert.assertTrue(record.get(key).contains(value));
		}
	}
	
	@Test
	public void testNonExistingFieldInteroperability(){
		record = newInstance();
		Assert.assertFalse(record.get(getKey()).contains(getForStorageValue()));
		Assert.assertTrue(record.fields().isEmpty());
	}
	
	@Test(expected=IllegalStateException.class)
	public void testAddExistingValueToKey(){
		record = newInstance();
		K key = getKey();
		V value = getForStorageValue();
		record.add(key, value);
		record.add(key, value);
	}
	
	@Test
	public void testAddMultipleKeys(){
		List<K> keys = Lists.newArrayList();
		record = newInstance();
		int count = Tests.getScaleCount();
		for(int i = 0; i < count; i++){
			K key = getKey();
			while(keys.contains(key)){
				key = getKey();
			}
			record.add(key, getForStorageValue());
		}
		for(K key : keys){
			Assert.assertTrue(record.fields().keySet().contains(key));
		}
	}
	
	
	

}
