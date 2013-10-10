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
import java.util.Set;

import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Sets;

/**
 * Base unit tests for {@link Store} services.
 * 
 * @author jnelson
 */
public abstract class StoreTest {

	protected Store store;

	@Rule
	public TestRule watcher = new TestWatcher() {

		@Override
		protected void starting(Description desc) {
			store = getStore();
			store.start();
		}

		@Override
		protected void finished(Description desc) {
			cleanup(store);
		}

	};

	@Test
	public void testVerifyEmpty() {
		Assert.assertFalse(store.verify(TestData.getString(),
				TestData.getTObject(), TestData.getLong()));
	}

	@Test
	public void testVerifyAfterAdd() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		Assert.assertTrue(store.verify(key, value, record));
	}

	@Test
	public void testVerifyAfterAddAndRemove() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		remove(key, value, record);
		Assert.assertFalse(store.verify(key, value, record));
	}

	@Test
	public void testVerifyAfterAddWithTime() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		long timestamp = Time.now();
		add(key, value, record);
		Assert.assertFalse(store.verify(key, value, record, timestamp));
	}

	@Test
	public void testVerifyAfterAddAndRemoveWithTime() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		long timestamp = Time.now();
		remove(key, value, record);
		Assert.assertTrue(store.verify(key, value, record, timestamp));
	}

	@Test
	public void testFetchEmptyKey() {
		Assert.assertTrue(store.fetch(TestData.getString(), TestData.getLong())
				.isEmpty());
	}

	@Test
	public void testFetchAfterAddSingleValue() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		Assert.assertTrue(store.fetch(key, record).contains(value));
	}

	@Test
	public void testFetchAfterAddMultiValues() {
		String key = TestData.getString();
		long record = TestData.getLong();
		Set<TObject> values = getValues();
		for (TObject value : values) {
			add(key, value, record);
		}
		Assert.assertEquals(values, store.fetch(key, record));
	}
	
	@Test
	public void testFetchAfterAddAndRemoveSingleValue(){
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		remove(key, value, record);
		Assert.assertFalse(store.fetch(key, record).contains(value));
	}
	
	@Test
	public void testFetchAfterAddMultiAndRemoveMulti(){
		String key = TestData.getString();
		long record = TestData.getLong();
		Set<TObject> values = getValues();
		for (TObject value : values) {
			add(key, value, record);
		}
		Iterator<TObject> it = values.iterator();
		while(it.hasNext()){
			TObject value = it.next();
			if(TestData.getInt() % 3 == 0){
				it.remove();
				remove(key, value, record);
			}
		}
		Assert.assertEquals(values, store.fetch(key, record));
	}

	/**
	 * Return a set of TObject values
	 * 
	 * @return the values
	 */
	private Set<TObject> getValues() {
		Set<TObject> values = Sets.newHashSet();
		for (int i = 0; i < TestData.getScaleCount(); i++) {
			TObject value = null;
			while (value == null | values.contains(value)) {
				value = TestData.getTObject();
			}
			values.add(value);
		}
		return values;
	}

	/**
	 * Return a Store for testing.
	 * 
	 * @return the Store
	 */
	protected abstract Store getStore();

	/**
	 * Cleanup the store and release and resources, etc.
	 * 
	 * @param store
	 */
	protected abstract void cleanup(Store store);

	/**
	 * Add {@code key} as {@code value} to {@code record} in the {@code store}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 */
	protected abstract void add(String key, TObject value, long record);

	/**
	 * Remove {@code key} as {@code value} from {@code record} in {@code store}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 */
	protected abstract void remove(String key, TObject value, long record);

}
