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
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import com.google.common.collect.Sets;

/**
 * Base unit tests for {@link Store} services.
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public abstract class StoreTest {

	@DataPoints
	public static Operator[] operators = { Operator.EQUALS,
			Operator.GREATER_THAN, Operator.GREATER_THAN_OR_EQUALS,
			Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUALS,
			Operator.NOT_EQUALS };

	@DataPoints
	public static SearchType[] searchTypes = { SearchType.PREFIX,
			SearchType.INFIX, SearchType.SUFFIX, SearchType.FULL };

	protected Store store;

	@Rule
	public TestRule watcher = new TestWatcher() {

		@Override
		protected void finished(Description desc) {
			cleanup(store);
		}

		@Override
		protected void starting(Description desc) {
			store = getStore();
			store.start();
		}

	};
	
	//TODO test audit

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
	public void testDescribeEmpty() {
		Assert.assertTrue(store.describe(TestData.getLong()).isEmpty());
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
	public void testFetchEmpty() {
		Assert.assertTrue(store.fetch(TestData.getString(), TestData.getLong())
				.isEmpty());
	}

	@Test
	@Theory
	public void testFind(Operator operator) {
		String key = TestData.getString();
		Number min = TestData.getNumber();
		Set<Long> records = addRecords(key, min, operator);
		Assert.assertEquals(records,
				store.find(key, operator, Convert.javaToThrift(min)));
	}

	@Test
	@Theory
	public void testFindAfterRemove(Operator operator) {
		String key = TestData.getString();
		Number min = TestData.getNumber();
		Set<Long> records = removeRecords(key, addRecords(key, min, operator));
		Assert.assertEquals(records,
				store.find(key, operator, Convert.javaToThrift(min)));
	}

	@Test
	@Theory
	public void testFindAfterRemoveWithTime(Operator operator) {
		String key = TestData.getString();
		Number min = TestData.getNumber();
		Set<Long> records = removeRecords(key, addRecords(key, min, operator));
		long timestamp = Time.now();
		removeRecords(key, addRecords(key, min, operator));
		Assert.assertEquals(records,
				store.find(timestamp, key, operator, Convert.javaToThrift(min)));
	}

	@Test
	@Theory
	public void testFindWithTime(Operator operator) {
		String key = TestData.getString();
		Number min = TestData.getNumber();
		Set<Long> records = addRecords(key, min, operator);
		long timestamp = Time.now();
		addRecords(key, min, operator);
		Assert.assertEquals(records,
				store.find(timestamp, key, operator, Convert.javaToThrift(min)));
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
	public void testPingEmpty() {
		Assert.assertFalse(store.ping(TestData.getLong()));
	}

	@Test
	@Theory
	public void testSearch(SearchType type) {
		String query = TestData.getString();
		String key = TestData.getString();
		Set<Long> records = setupSearchTest(key, query, type);
		Assert.assertEquals(records, store.search(key, query));
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
	public void testVerifyAfterAddWithTime() {
		String key = TestData.getString();
		TObject value = TestData.getTObject();
		long record = TestData.getLong();
		long timestamp = Time.now();
		add(key, value, record);
		Assert.assertFalse(store.verify(key, value, record, timestamp));
	}

	@Test
	public void testVerifyEmpty() {
		Assert.assertFalse(store.verify(TestData.getString(),
				TestData.getTObject(), TestData.getLong()));
	}

	/**
	 * Add {@code key} as {@code value} to {@code record} in the {@code store}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 */
	protected abstract void add(String key, TObject value, long record);

	/**
	 * Cleanup the store and release and resources, etc.
	 * 
	 * @param store
	 */
	protected abstract void cleanup(Store store);

	/**
	 * Return a Store for testing.
	 * 
	 * @return the Store
	 */
	protected abstract Store getStore();

	/**
	 * Remove {@code key} as {@code value} from {@code record} in {@code store}.
	 * 
	 * @param key
	 * @param value
	 * @param record
	 */
	protected abstract void remove(String key, TObject value, long record);

	/**
	 * Add {@code key} as a value that satisfies {@code operator} relative to
	 * {@code min}.
	 * 
	 * @param key
	 * @param min
	 * @param operator
	 * @return the records added
	 */
	private Set<Long> addRecords(String key, Number min, Operator operator) {
		Set<Long> records = getRecords();
		for (long record : records) {
			Number n = null;
			while (n == null
					|| (operator == Operator.GREATER_THAN && Numbers
							.isLessThanOrEqualTo(n, min))
					|| (operator == Operator.GREATER_THAN_OR_EQUALS && Numbers
							.isLessThan(n, min))
					|| (operator == Operator.LESS_THAN && Numbers
							.isGreaterThanOrEqualTo(n, min))
					|| (operator == Operator.LESS_THAN_OR_EQUALS && Numbers
							.isGreaterThan(n, min))
					|| (operator == Operator.NOT_EQUALS && Numbers.isEqualTo(n,
							min))
					|| (operator == Operator.EQUALS && !Numbers.isEqualTo(n,
							min))) {
				n = operator == Operator.EQUALS ? min : TestData.getNumber();
			}
			add(key, Convert.javaToThrift(n), record);
		}
		return records;
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
	 * Remove {@code key} from a random sample of {@code records}.
	 * 
	 * @param key
	 * @param records
	 * @return the records that remain after the function
	 */
	private Set<Long> removeRecords(String key, Set<Long> records) {
		Iterator<Long> it = records.iterator();
		while (it.hasNext()) {
			long record = it.next();
			if(TestData.getInt() % 3 == 0) {
				TObject value = store.fetch(key, record).iterator().next();
				it.remove();
				remove(key, value, record);
			}
		}
		return records;
	}

	/**
	 * Setup a search test by adding some random matches for {@code query} that
	 * obey search {@code type} for {@code key} in a random set of records.
	 * 
	 * @param key
	 * @param query
	 * @param type
	 * @return the records where the query matches
	 */
	private Set<Long> setupSearchTest(String key, String query, SearchType type) {
		Set<Long> records = Sets.newHashSet();
		for (long record : getRecords()) {
			String other = null;
			while (other == null || other.equals(query)
					|| query.contains(other)) {
				other = TestData.getString();
			}
			boolean match = TestData.getInt() % 3 == 0;
			if(match && type == SearchType.PREFIX) {
				add(key, Convert.javaToThrift(query + other), record);
				records.add(record);
			}
			else if(match && type == SearchType.SUFFIX) {
				add(key, Convert.javaToThrift(other + query), record);
				records.add(record);
			}
			else if(match && type == SearchType.INFIX) {
				add(key, Convert.javaToThrift(other + query + other), record);
				records.add(record);
			}
			else if(match && type == SearchType.FULL) {
				add(key, Convert.javaToThrift(query), record);
				records.add(record);
			}
			else {
				add(key, Convert.javaToThrift(other), record);
			}
		}
		return records;
	}

	/**
	 * List of search types
	 * 
	 * @author jnelson
	 */
	private enum SearchType {
		PREFIX, INFIX, SUFFIX, FULL
	}

}
