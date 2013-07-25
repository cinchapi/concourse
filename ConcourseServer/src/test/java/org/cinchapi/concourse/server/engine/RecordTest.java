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
import java.util.Map;
import java.util.Map.Entry;
import junit.framework.Assert;

import org.cinchapi.common.io.Byteable;
import org.cinchapi.common.tests.BaseTest;
import org.cinchapi.common.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Maps;

/**
 * Base unit tests for {@link Record} implementations.
 * 
 * @author jnelson
 */
public abstract class RecordTest<L extends Byteable, K extends Byteable, V extends Storable>
		extends BaseTest {

	/**
	 * The record that is being used in the current test. This means that there
	 * should only be one test used per method.
	 */
	private Record<L, K, V> record = null;

	@Rule
	public TestWatcher myWatcher = new TestWatcher() {

		@Override
		protected void finished(Description description) {
			if(record != null) {
				record.delete();
				log.info("{} has been deleted", record);
			}
			record = null;
		}

	};

	@Test
	public void testAddToPopulatedRecord() {

	}

	@Test
	public void testCanAddMultipleDistinctValuesToField() {
		record = getInstance();
		K key = getKey();
		int count = Random.getScaleCount();
		log.info("Adding {} values", count);
		for (int i = 0; i < count; i++) {
			V value = getValue();
			while (record.get(key).contains(value)) {
				log.info("'{}' is already contained in the field", value);
				value = getValue();
			}
			record.add(key, value);
		}
		Assert.assertEquals(count, record.get(key).count());
	}

	@Test(expected = IllegalStateException.class)
	public void testCannnotAddDuplicateValueToField() {
		record = getInstance();
		K key = getKey();
		V value = getValue();
		record.add(key, value);
		record.add(key, value);
	}

	@Test
	public void testFsync() {
		L locator = getLocator();
		record = getInstance(locator);
		populateWithAddAndRemove(record);
		record.fsync();
		Record<L, K, V> copy = getInstance(locator);
		Assert.assertEquals(record, copy);
	}

	@Test
	public void testInitialAdd() {
		record = getInstance();
		K key = getKey();
		V value = getValue();
		record.add(key, value);
		Assert.assertTrue(record.get(key).contains(value));
	}

	@Test
	public void testInitialRemove() {
		K key = getKey();
		V value = getValue();
		record = getInstanceWith(key, value);
		record.remove(key, copy(value));
		Assert.assertFalse(record.get(key).contains(value));
	}

	@Test
	public void testNonExistingFieldInteroperability() {
		record = getInstance();
		K key = getKey();
		Assert.assertFalse(record.get(key).contains(getValue()));
		Assert.assertTrue(record.fields.isEmpty());
	}

	/**
	 * Return a copy of {@code value} with a unique timestamp.
	 * 
	 * @param value
	 * @return the copy of {@code value}
	 */
	protected abstract V copy(V value);

	/**
	 * Return an instance.
	 * 
	 * @return the instance
	 */
	protected Record<L, K, V> getInstance() {
		return getInstance(getLocator());
	}

	/**
	 * Return an instance.
	 * 
	 * @return the instance
	 */
	protected abstract Record<L, K, V> getInstance(L locator);

	/**
	 * Return a record with {@code key} as {@code value}.
	 * 
	 * @param key
	 * @param value
	 * @return the instance
	 */
	protected Record<L, K, V> getInstanceWith(K key, V value) {
		Record<L, K, V> record = getInstance();
		record.add(key, value);
		return record;
	}

	/**
	 * Return a key.
	 * 
	 * @return the Key
	 */
	protected abstract K getKey();

	/**
	 * Return a locator.
	 * 
	 * @return the Locator
	 */
	protected abstract L getLocator();

	/**
	 * Return a value.
	 * 
	 * @return the Value
	 */
	protected abstract V getValue();

	/**
	 * Populate {@code record} with ADD writes.
	 * 
	 * @param record
	 * @return the mappings contained in {@code record}
	 */
	protected Map<K, V> populateWithAdd(Record<L, K, V> record) {
		Map<K, V> writes = Maps.newHashMap();
		int count = Random.getScaleCount();
		for (int i = 0; i < count; i++) {
			K key = getKey();
			V value = getValue();
			while (record.get(key).contains(value)) {
				value = getValue();
			}
			writes.put(key, value);
			record.add(key, value);
		}
		return writes;
	}

	/**
	 * Populate {@code record} with ADD and REMOVE writes.
	 * 
	 * @param record
	 * @return the mappings contained in {@code record}
	 */
	protected Map<K, V> populateWithAddAndRemove(Record<L, K, V> record) {
		Map<K, V> writes = populateWithAdd(record);
		Iterator<Entry<K, V>> it = writes.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, V> entry = it.next();
			if(Random.getInt() % 2 == 0) {
				record.remove(entry.getKey(), copy(entry.getValue()));
				it.remove();
			}
		}
		return writes;
	}

	// @Test
	// public void testAddMultipleKeys(){
	// List<K> keys = Lists.newArrayList();
	// record = newInstance();
	// int count = Tests.getScaleCount();
	// for(int i = 0; i < count; i++){
	// K key = getKey();
	// while(keys.contains(key)){
	// key = getKey();
	// }
	// record.add(key, getForStorageValue());
	// }
	// for(K key : keys){
	// Assert.assertTrue(record.fields().keySet().contains(key));
	// }
	// }
	//
	//
	//

}
