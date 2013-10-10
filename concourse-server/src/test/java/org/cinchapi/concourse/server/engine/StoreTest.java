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

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
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
	public void testFindEquals() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		Set<Long> records = getRecords();
		for (long record : records) {
			add(key, value, record);
		}
		Assert.assertEquals(records, store.find(key, Operator.EQUALS, value));
	}

	@Test
	public void testPingEmpty() {
		Assert.assertFalse(store.ping(TestData.getLong()));
	}

	@Test
	public void testPingAfterAdd() {
		long record = TestData.getLong();
		add(TestData.getString(), TestData.getTObject(), record);
		Assert.assertTrue(store.ping(record));
	}

	@Test
	public void testPingAfterAddAndRemove() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		remove(key, value, record);
		Assert.assertFalse(store.ping(record));
	}

	@Test
	public void testDescribeEmpty() {
		Assert.assertTrue(store.describe(TestData.getLong()).isEmpty());
	}

	@Test
	public void testDescribeAfterAddSingle() {
		String key = TestData.getString();
		long record = TestData.getLong();
		add(key, TestData.getTObject(), record);
		Assert.assertTrue(store.describe(record).contains(key));
	}

	@Test
	public void testDescribeAfterAddSingleWithTime() {
		String key = TestData.getString();
		long record = TestData.getLong();
		long timestamp = Time.now();
		add(key, TestData.getTObject(), record);
		Assert.assertFalse(store.describe(record, timestamp).contains(key));
	}

	@Test
	public void testDescribeAfterAddAndRemoveSingle() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		remove(key, value, record);
		Assert.assertFalse(store.describe(record).contains(key));
	}

	@Test
	public void testDescribeAfterAddAndRemoveSingleWithTime() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		long timestamp = Time.now();
		remove(key, value, record);
		Assert.assertTrue(store.describe(record, timestamp).contains(key));
	}

	@Test
	public void testDescribeAfterAddMulti() {
		long record = TestData.getLong();
		Set<String> keys = getKeys();
		for (String key : keys) {
			add(key, TestData.getTObject(), record);
		}
		Assert.assertEquals(keys, store.describe(record));
	}

	@Test
	public void testDescribeAfterAddMultiWithTime() {
		long record = TestData.getLong();
		Set<String> keys = getKeys();
		for (String key : keys) {
			add(key, TestData.getTObject(), record);
		}
		long timestamp = Time.now();
		for (String key : getKeys()) {
			add(key, TestData.getTObject(), record);
		}
		Assert.assertEquals(keys, store.describe(record, timestamp));
	}

	@Test
	public void testDescribeAfterAddMultiAndRemoveMulti() {
		long record = TestData.getLong();
		Set<String> keys = getKeys();
		int count = 0;
		for (String key : keys) {
			add(key, Convert.javaToThrift(count), record);
			count++;
		}
		Iterator<String> it = keys.iterator();
		count = 0;
		while (it.hasNext()) {
			String key = it.next();
			if(TestData.getInt() % 3 == 0) {
				it.remove();
				remove(key, Convert.javaToThrift(count), record);
			}
			count++;
		}
		Assert.assertEquals(keys, store.describe(record));
	}

	@Test
	public void testDescribeAfterAddMultiAndRemoveMultiWithTime() {
		long record = TestData.getLong();
		Set<String> keys = getKeys();
		int count = 0;
		for (String key : keys) {
			add(key, Convert.javaToThrift(count), record);
			count++;
		}
		Iterator<String> it = keys.iterator();
		count = 0;
		while (it.hasNext()) {
			String key = it.next();
			if(TestData.getInt() % 3 == 0) {
				it.remove();
				remove(key, Convert.javaToThrift(count), record);
			}
			count++;
		}
		long timestamp = Time.now();
		count = 0;
		for (String key : getKeys()) {
			add(key, Convert.javaToThrift(count), record);
			count++;
		}
		Assert.assertEquals(keys, store.describe(record, timestamp));
	}

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
	public void testFetchEmpty() {
		Assert.assertTrue(store.fetch(TestData.getString(), TestData.getLong())
				.isEmpty());
	}

	@Test
	public void testFetchAfterAddSingle() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		Assert.assertTrue(store.fetch(key, record).contains(value));
	}

	@Test
	public void testFetchAfterAddSingleWithTime() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		long timestamp = Time.now();
		add(key, value, record);
		Assert.assertFalse(store.fetch(key, record, timestamp).contains(value));
	}

	@Test
	public void testFetchAfterAddMulti() {
		String key = TestData.getString();
		long record = TestData.getLong();
		Set<TObject> values = getValues();
		for (TObject value : values) {
			add(key, value, record);
		}
		Assert.assertEquals(values, store.fetch(key, record));
	}

	@Test
	public void testFetchAfterAddMultiWithTime() {
		String key = TestData.getString();
		long record = TestData.getLong();
		Set<TObject> values = getValues();
		for (TObject value : values) {
			add(key, value, record);
		}
		long timestamp = Time.now();
		for (TObject value : getValues()) {
			add(key, value, record);
		}
		Assert.assertEquals(values, store.fetch(key, record, timestamp));
	}

	@Test
	public void testFetchAfterAddAndRemoveSingle() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		remove(key, value, record);
		Assert.assertFalse(store.fetch(key, record).contains(value));
	}

	@Test
	public void testFetchAfterAddAndRemoveSingleWithTime() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		add(key, value, record);
		long timestamp = Time.now();
		remove(key, value, record);
		Assert.assertTrue(store.fetch(key, record, timestamp).contains(value));
	}

	@Test
	public void testFetchAfterAddMultiAndRemoveMulti() {
		String key = TestData.getString();
		long record = TestData.getLong();
		Set<TObject> values = getValues();
		for (TObject value : values) {
			add(key, value, record);
		}
		Iterator<TObject> it = values.iterator();
		while (it.hasNext()) {
			TObject value = it.next();
			if(TestData.getInt() % 3 == 0) {
				it.remove();
				remove(key, value, record);
			}
		}
		Assert.assertEquals(values, store.fetch(key, record));
	}

	@Test
	public void testFetchAfterAddMultiAndRemoveMultiWithTime() {
		String key = TestData.getString();
		long record = TestData.getLong();
		Set<TObject> values = getValues();
		for (TObject value : values) {
			add(key, value, record);
		}
		Iterator<TObject> it = values.iterator();
		while (it.hasNext()) {
			TObject value = it.next();
			if(TestData.getInt() % 3 == 0) {
				it.remove();
				remove(key, value, record);
			}
		}
		long timestamp = Time.now();
		for (TObject value : getValues()) {
			add(key, value, record);
		}
		for (TObject value : getValues()) {
			remove(key, value, record);
		}
		Assert.assertEquals(values, store.fetch(key, record, timestamp));
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
			while (value == null || values.contains(value)) {
				value = TestData.getTObject();
			}
			values.add(value);
		}
		return values;
	}

	/**
	 * Return a set of keys.
	 * 
	 * @return the keys.
	 */
	private Set<String> getKeys() {
		Set<String> keys = Sets.newHashSet();
		for (int i = 0; i < TestData.getScaleCount(); i++) {
			String key = null;
			while (key == null || keys.contains(key)) {
				key = TestData.getString();
			}
			keys.add(key);
		}
		return keys;
	}

	/**
	 * Return a set of primary keys.
	 * 
	 * @return the records
	 */
	private Set<Long> getRecords() {
		Set<Long> records = Sets.newHashSet();
		for (int i = 0; i < TestData.getScaleCount(); i++) {
			Long record = null;
			while (record == null || records.contains(record)) {
				record = TestData.getLong();
			}
			records.add(record);
		}
		return records;
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
