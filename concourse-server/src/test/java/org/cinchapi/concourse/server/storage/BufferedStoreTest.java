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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Numbers;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.Theory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
public abstract class BufferedStoreTest extends StoreTest {

	private static final List<String> POSSIBLE_KEYS = Lists.newArrayList("A",
			"B", "C", "D");
	private static final List<TObject> POSSIBLE_VALUES = Lists.newArrayList(
			Convert.javaToThrift("one"), Convert.javaToThrift("two"),
			Convert.javaToThrift("three"), Convert.javaToThrift("four"),
			Convert.javaToThrift("five"), Convert.javaToThrift("six"),
			Convert.javaToThrift("seven"), Convert.javaToThrift("eight"),
			Convert.javaToThrift("nine"), Convert.javaToThrift("ten"));
	private static final List<Long> POSSIBLE_RECORDS = Lists.newArrayList(1L,
			2L, 3L, 4L, 5L, 6L, 7L);

	@Test
	@Theory
	public void testFindBS(Operator operator) {
//		List<Data> data = generateTestData();
//		insertData(data);
//		Data d = Variables.register("d",
//				data.get(TestData.getScaleCount() % data.size()));
//		Map<Long, Set<TObject>> rtv = Maps.newHashMap();
//		Iterator<Data> it = data.iterator();
//		Variables.register("operator", operator);
//		while (it.hasNext()) {
//			Data _d = it.next();
//			if(_d.key.equals(d.key)) {
//				boolean matches = false;
//				if(operator == Operator.EQUALS) {
//					matches = d.value.equals(_d.value);
//				}
//				else if(operator == Operator.NOT_EQUALS) {
//					matches = !d.value.equals(_d.value);
//				}
//				else if(operator == Operator.GREATER_THAN) {
//					matches = d.value.compareTo(_d.value) < 0;
//				}
//				else if(operator == Operator.GREATER_THAN_OR_EQUALS) {
//					matches = d.value.compareTo(_d.value) <= 0;
//				}
//				else if(operator == Operator.LESS_THAN) {
//					matches = d.value.compareTo(_d.value) > 0;
//				}
//				else if(operator == Operator.LESS_THAN_OR_EQUALS) {
//					matches = d.value.compareTo(_d.value) >= 0;
//				}
//				else if(operator == Operator.BETWEEN) {
//					// TODO skip for now
//				}
//				else if(operator == Operator.REGEX) {
//					matches = _d.value.toString().matches(d.value.toString());
//				}
//				else if(operator == Operator.NOT_REGEX) {
//					matches = !_d.value.toString().matches(d.value.toString());
//				}
//				else {
//					throw new UnsupportedOperationException();
//				}
//				if(matches) {
//					Set<TObject> values = rtv.get(_d.record);
//					if(values == null) {
//						values = Sets.newHashSet();
//						rtv.put(_d.record, values);
//					}
//					if(_d.type == Action.ADD) {
//						Assert.assertTrue(values.add(_d.value));
//					}
//					else {
//						Assert.assertTrue(values.remove(_d.value));
//					}
//				}
//
//			}
		}
		Set<Long> records = Sets.newHashSet();
		Iterator<Long> it2 = rtv.keySet().iterator();
		while (it2.hasNext()) {
			long record = it2.next();
			if(!rtv.get(record).isEmpty()) {
				records.add(record);
			}
		}
		Assert.assertEquals(records, store.find(d.key, operator, d.value));
	}

	@Test
	public void testVerifyBS() {
		List<Data> data = generateTestData();
		insertData(data);
		Data d = Variables.register("d",
				data.get(TestData.getScaleCount() % data.size()));
		boolean verify = Numbers.isOdd(count(data, d));
		Assert.assertEquals(verify, store.verify(d.key, d.value, d.record));
	}

	@Test
	public void testVerifyBSReproA() {
		String orderString = "ADD C AS nine IN 5, ADD C AS five IN 4, ADD B AS ten IN 6, "
				+ "ADD B AS two IN 1, ADD C AS five IN 7, REMOVE C AS five IN 7, ADD C AS "
				+ "seven IN 5, ADD B AS ten IN 2, ADD C AS nine IN 3, ADD C AS nine IN 2, "
				+ "ADD A AS seven IN 3, ADD B AS ten IN 1, ADD B AS eight IN 3, ADD C AS one "
				+ "IN 3, ADD D AS eight IN 7, ADD C AS three IN 2, ADD D AS ten IN 6, REMOVE "
				+ "D AS eight IN 7, ADD A AS three IN 6, ADD A AS nine IN 2, ADD D AS two IN "
				+ "4, ADD B AS two IN 6, ADD D AS four IN 4, REMOVE C AS one IN 3, ADD A AS "
				+ "three IN 5, ADD B AS ten IN 3, ADD A AS five IN 4, ADD C AS five IN 5, "
				+ "ADD D AS eight IN 6, ADD C AS three IN 1, ADD B AS four IN 5, ADD B AS "
				+ "four IN 4, REMOVE B AS eight IN 3, ADD D AS four IN 7, ADD A AS nine IN "
				+ "5, ADD C AS three IN 7, ADD C AS five IN 1, ADD A AS nine IN 1, REMOVE C "
				+ "AS nine IN 2, ADD B AS eight IN 1, REMOVE B AS ten IN 1, ADD A AS nine "
				+ "IN 7, REMOVE C AS seven IN 5, ADD D AS two IN 2, ADD B AS eight IN 4, ADD "
				+ "C AS nine IN 4, REMOVE C AS five IN 1, ADD D AS six IN 6, ADD D AS ten IN "
				+ "5, REMOVE C AS five IN 4, ADD B AS six IN 6, ADD C AS three IN 3, ADD B AS "
				+ "two IN 7, ADD D AS two IN 1, REMOVE C AS five IN 5, ADD D AS six IN 1, "
				+ "REMOVE C AS three IN 1, ADD C AS five IN 6, REMOVE A AS nine IN 5, "
				+ "ADD A AS one IN 1, ADD D AS two IN 3, REMOVE C AS nine IN 5, ADD B "
				+ "AS six IN 2, REMOVE B AS four IN 4, ADD A AS three IN 4, ADD C AS "
				+ "seven IN 4, ADD C AS one IN 4, REMOVE C AS three IN 3, ADD C AS "
				+ "seven IN 3, REMOVE C AS three IN 2, ADD D AS four IN 2, ADD D AS ten "
				+ "IN 3, REMOVE B AS eight IN 4, ADD D AS eight IN 4, REMOVE D AS eight "
				+ "IN 4, ADD A AS three IN 2, REMOVE A AS three IN 5, ADD B AS six IN 5,"
				+ " REMOVE D AS six IN 1, ADD B AS four IN 7, REMOVE B AS four IN 5, ADD"
				+ " A AS five IN 3, ADD B AS eight IN 2, REMOVE D AS ten IN 6, ADD A AS "
				+ "seven IN 2, REMOVE D AS six IN 6, ADD A AS seven IN 1, ADD C AS three "
				+ "IN 6, ADD D AS four IN 1, REMOVE A AS three IN 2, ADD D AS ten IN 4, "
				+ "REMOVE D AS ten IN 5, ADD A AS one IN 4, REMOVE D AS four IN 2, ADD A "
				+ "AS five IN 2, ADD C AS one IN 7, ADD C AS one IN 1, REMOVE B AS six IN"
				+ " 5, ADD A AS five IN 5, ADD A AS one IN 6, REMOVE C AS three IN 7, ADD"
				+ " D AS six IN 5, REMOVE B AS two IN 7, ADD D AS four IN 3, REMOVE B AS eight "
				+ "IN 1, ADD B AS four IN 3, REMOVE A AS five IN 2, ADD B AS four IN 6, REMOVE "
				+ "B AS two IN 1, ADD A AS five IN 1, REMOVE A AS three IN 4, ADD D AS eight "
				+ "IN 1, ADD A AS one IN 7, REMOVE D AS four IN 3, ADD A AS seven IN 7, REMOVE"
				+ " C AS nine IN 4, ADD A AS one IN 5, ADD A AS three IN 3, REMOVE C AS seven "
				+ "IN 3, ADD B AS ten IN 7, REMOVE A AS five IN 4, ADD B AS six IN 3, REMOVE C"
				+ " AS nine IN 3, ADD D AS six IN 7, REMOVE D AS two IN 4, ADD C AS one IN 2,"
				+ " REMOVE D AS eight IN 6, ADD B AS six IN 4, REMOVE A AS seven IN 3, ADD D "
				+ "AS six IN 2, REMOVE B AS six IN 3, ADD D AS two IN 5, REMOVE D AS four IN "
				+ "7, ADD B AS two IN 5, REMOVE A AS seven IN 2, ADD D AS eight IN 5, REMOVE"
				+ " C AS five IN 6, ADD A AS nine IN 6, REMOVE D AS two IN 2, ADD B AS two IN "
				+ "2, REMOVE D AS two IN 1, ADD C AS seven IN 7, REMOVE B AS four IN 7, ADD C "
				+ "AS seven IN 6, REMOVE C AS seven IN 6, REMOVE C AS one IN 2, REMOVE D AS"
				+ " eight IN 5, REMOVE A AS one IN 7, REMOVE A AS one IN 6, REMOVE A AS seven "
				+ "IN 7, REMOVE B AS six IN 4, REMOVE B AS ten IN 2, REMOVE A AS nine IN 1, "
				+ "REMOVE A AS nine IN 6, REMOVE D AS six IN 7, REMOVE B AS two IN 6, REMOVE "
				+ "A AS one IN 4, REMOVE A AS one IN 1, REMOVE B AS ten IN 3, REMOVE A AS seven"
				+ " IN 1, REMOVE C AS one IN 7, REMOVE A AS five IN 1, REMOVE D AS eight IN 1, "
				+ "REMOVE B AS ten IN 7, REMOVE C AS seven IN 7, REMOVE A AS nine IN 7, REMOVE "
				+ "C AS three IN 6, REMOVE B AS eight IN 2, REMOVE D AS two IN 3, REMOVE D AS "
				+ "ten IN 4, REMOVE D AS two IN 5, REMOVE A AS five IN 5, REMOVE D AS four IN 1,"
				+ " REMOVE A AS three IN 6, REMOVE A AS three IN 3, REMOVE B AS four IN 3, "
				+ "REMOVE C AS one IN 1, REMOVE B AS six IN 6, REMOVE D AS six IN 2, REMOVE "
				+ "B AS six IN 2, REMOVE A AS nine IN 2, REMOVE A AS five IN 3, REMOVE B AS "
				+ "ten IN 6, REMOVE B AS two IN 2, REMOVE B AS four IN 6, REMOVE C AS one IN "
				+ "4, REMOVE D AS ten IN 3, REMOVE B AS two IN 5, REMOVE D AS four IN 4,"
				+ " REMOVE C AS seven IN 4, REMOVE A AS one IN 5";
		List<Data> data = recoverTestData(orderString);
		insertData(data, 83);
		Data d = Variables.register("d", Data.fromString("ADD A AS nine IN 1"));
		boolean verify = Numbers.isOdd(count(data, d));
		Assert.assertEquals(verify, store.verify(d.key, d.value, d.record));
	}

	@Test
	public void testFetchBS() {
		List<Data> data = generateTestData();
		insertData(data);
		Data d = Variables.register("d",
				data.get(TestData.getScaleCount() % data.size()));
		Set<TObject> values = Sets.newHashSet();
		Iterator<Data> it = data.iterator();
		while (it.hasNext()) {
			Data _d = it.next();
			if(_d.record == d.record && _d.key.equals(d.key)) {
				if(_d.type == Action.ADD) {
					Assert.assertTrue(values.add(_d.value));
				}
				else {
					Assert.assertTrue(values.remove(_d.value));
				}
			}
		}
		Assert.assertEquals(values, store.fetch(d.key, d.record));
	}

	@Test
	public void testDescribeBS() {
		List<Data> data = generateTestData();
		insertData(data);
		Data d = data.get(TestData.getScaleCount() % data.size());
		Map<String, Set<TObject>> ktv = Maps.newHashMap();
		Iterator<Data> it = data.iterator();
		while (it.hasNext()) {
			Data _d = it.next();
			if(_d.record == d.record) {
				Set<TObject> values = ktv.get(_d.key);
				if(values == null) {
					values = Sets.newHashSet();
					ktv.put(_d.key, values);
				}
				if(_d.type == Action.ADD) {
					Assert.assertTrue(values.add(_d.value));
				}
				else {
					Assert.assertTrue(values.remove(_d.value));
				}
			}
		}
		Set<String> keys = Sets.newHashSet();
		Iterator<String> it2 = ktv.keySet().iterator();
		while (it2.hasNext()) {
			String key = it2.next();
			if(!ktv.get(key).isEmpty()) {
				keys.add(key);
			}
		}
		Assert.assertEquals(keys, store.describe(d.record));
	}

	private void insertData(List<Data> data, int numForDestination) {
		Variables.register("numForDestination", numForDestination);
		Iterator<Data> it = data.iterator();
		for (int i = 0; i < numForDestination; i++) {
			Data d = it.next();
			if(d.type == Action.ADD) {
				((BufferedStore) store).destination.accept(Write.add(d.key,
						d.value, d.record));
			}
			else {
				((BufferedStore) store).destination.accept(Write.remove(d.key,
						d.value, d.record));
			}
		}
		while (it.hasNext()) {
			Data d = it.next();
			if(d.type == Action.ADD) {
				((BufferedStore) store).buffer.insert(Write.add(d.key, d.value,
						d.record));
			}
			else {
				((BufferedStore) store).buffer.insert(Write.remove(d.key,
						d.value, d.record));
			}
		}
	}

	/**
	 * Insert {@code data} into the BufferedStore.
	 * 
	 * @param data
	 */
	private void insertData(List<Data> data) {
		insertData(data, TestData.getScaleCount() % data.size());
	}

	/**
	 * Generate data to insert in the BufferedStore.
	 * 
	 * @return the data
	 */
	private List<Data> generateTestData() {
		// Setup iterators
		Iterator<String> keys = Iterators.cycle(POSSIBLE_KEYS);
		Iterator<TObject> values = Iterators.cycle(POSSIBLE_VALUES);
		Iterator<Long> records = Iterators.cycle(POSSIBLE_RECORDS);

		// Get initial positive and negative data so we can guarantee that every
		// remove offsets an add
		int numNegData = TestData.getScaleCount();
		List<Data> posData = Lists.newArrayList();
		List<Data> negData = Lists.newArrayList();
		for (int i = 0; i < numNegData; i++) {
			Data pos = Data
					.positive(keys.next(), values.next(), records.next());
			Data neg = Data.negative(pos);
			posData.add(pos);
			negData.add(neg);
		}

		// Get more positive data, no greater than the number of available keys
		// (the smallest list) so that we can guarantee that we don't have any
		// adds that aren't offset
		int numAddlPosData = TestData.getScaleCount() % POSSIBLE_KEYS.size();
		for (int i = 0; i < numAddlPosData; i++) {
			Data pos = Data
					.positive(keys.next(), values.next(), records.next());
			posData.add(pos);
		}

		// Create the order in which the data will be written
		List<Data> order = Lists.newArrayList();
		boolean lastWasNeg = true;
		while (posData.size() > 0 || negData.size() > 0) {
			if(lastWasNeg && posData.size() > 0) {
				int index = TestData.getScaleCount() % posData.size();
				if(Numbers.isEven(count(order, posData.get(index)))) {
					order.add(posData.get(index));
					posData.remove(index);
				}
				lastWasNeg = false;
			}
			else {
				if(negData.size() > 0) {
					int index = TestData.getScaleCount() % negData.size();
					if(Numbers.isOdd(count(order, negData.get(index)))) {
						order.add(negData.get(index));
						negData.remove(index);
					}
					lastWasNeg = true;
				}
			}
		}
		return Variables.register("order", order);
	}

	private int count(List<Data> data, Data element) {
		int i = 0;
		for (Data d : data) {
			i += d.equals(element) ? 1 : 0;
		}
		return i;
	}

	private List<Data> recoverTestData(String orderString) {
		orderString.replace("]", "");
		orderString.replace("[", "");
		String[] toks = orderString.split(",");
		List<Data> data = Lists.newArrayList();
		for (String tok : toks) {
			data.add(Data.fromString(tok));
		}
		Variables.register("order", data);
		return data;
	}

	/**
	 * A test class that encapsulates data that is added to the BufferedStore.
	 * 
	 * @author jnelson
	 */
	private static final class Data {

		/**
		 * Return the Data element that is described by {@code string}
		 * 
		 * @param string
		 * @return the data
		 */
		public static Data fromString(String string) {
			string = string.trim();
			String[] toks = string.split(" ");
			return new Data(Action.valueOf(toks[0]), toks[1],
					Convert.javaToThrift(toks[3]), Long.valueOf(toks[5]));
		}

		/**
		 * Return a positive Data element.
		 * 
		 * @param key
		 * @param value
		 * @param record
		 * @return the data
		 */
		public static Data positive(String key, TObject value, long record) {
			return new Data(Action.ADD, key, value, record);
		}

		/**
		 * Return a negative Data element that is an offset of {@code data}.
		 * 
		 * @param data
		 * @return the negative offset for {@code data}
		 */
		public static Data negative(Data data) {
			return new Data(Action.REMOVE, data.key, data.value, data.record);

		}

		private final long record;
		private final String key;
		private final TObject value;
		private final Action type;

		/**
		 * Construct a new instance.
		 * 
		 * @param type
		 * @param key
		 * @param value
		 * @param record
		 */
		private Data(Action type, String key, TObject value, long record) {
			this.type = type;
			this.key = key;
			this.value = value;
			this.record = record;
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, value, record);
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Data) {
				return ((Data) obj).key.equals(key)
						&& ((Data) obj).value.equals(value)
						&& ((Data) obj).record == record;
			}
			return false;
		}

		@Override
		public String toString() {
			return type + " " + key + " AS " + value + " IN " + record;
		}

	}

}
