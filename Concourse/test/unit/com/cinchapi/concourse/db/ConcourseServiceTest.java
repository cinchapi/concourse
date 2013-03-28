/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.db;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.db.ConcourseService;
import com.cinchapi.concourse.db.IndexingService;
import com.cinchapi.concourse.db.QueryService.Operator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@AbstractConcourseService}.
 * 
 * @author jnelson
 */
public abstract class ConcourseServiceTest extends DbBaseTest {

	protected abstract ConcourseService getService();

	@Test
	public void testAdd() {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();
		Object value = randomObject();
		assertTrue(service.add(column, value, row));

		// can't add dupes
		assertFalse(service.add(column, value, row));

		// multiple values in column
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Object v = randomObject();
			while (service.exists(column, v, row)) {
				v = randomObject();
			}
			assertTrue(service.add(column, v, row));
		}

		// multiple columns in a row
		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			String c = randomColumnName();
			while (service.exists(c, row)) {
				c = randomColumnName();
			}
			assertTrue(service.add(c, value, row));
		}

		// multiple rows
		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			long r = randomLong();
			while (service.exists(r)) {
				r = randomLong();
			}
			assertTrue(service.add(column, value, r));
		}

	}

	@Test
	public void testDescribe() {
		ConcourseService service = getService();

		long row = randomLong();

		int scale = randomScaleFrequency();
		Set<String> columns = Sets.newTreeSet();
		for (int i = 0; i < scale; i++) {
			String column = randomColumnName();
			while (columns.contains(column)) {
				column = randomColumnName();
			}
			columns.add(column);
			service.add(column, randomObject(), row);
		}
		assertEquals(columns, service.describe(row));
	}

	@Test
	public void testExists() {
		ConcourseService service = getService();

		// row exists
		long row = randomLong();
		String column = randomColumnName();
		Object value = randomObject();

		assertFalse(service.exists(row));
		service.add(column, value, row);
		assertTrue(service.exists(row));

		// row doesn't exists after being removed
		service.remove(column, value, row);
		assertFalse(service.exists(row));

		// adding and removing at scale yields correct exists value for row
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			if(Numbers.isEven(i)) {
				service.add(column, value, row);
			}
			else {
				service.remove(column, value, row);
			}
		}
		assertEquals(service.exists(row), Numbers.isEven(scale) ? false : true);

		// row and column exist
		row = randomLong();
		column = randomColumnName();
		value = randomObject();

		assertFalse(service.exists(column, row));
		service.add(column, value, row);
		assertTrue(service.exists(column, row));

		// row and column don't exist after being removed
		service.remove(column, value, row);
		assertFalse(service.exists(column, row));
		service.add(randomColumnName(), value, row);
		assertFalse(service.exists(column, row));

		// adding and removing at scale yields correct exists value for row and
		// column
		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			if(Numbers.isEven(i)) {
				service.add(column, value, row);
			}
			else {
				service.remove(column, value, row);
			}
		}
		assertEquals(service.exists(column, row), Numbers.isEven(scale) ? false
				: true);

		// row and column and value exist
		row = randomLong();
		column = randomColumnName();
		value = randomObject();

		assertFalse(service.exists(column, value, row));
		service.add(column, value, row);
		assertTrue(service.exists(column, value, row));

		// row and column and value don't exist after being removed
		service.remove(column, value, row);
		assertFalse(service.exists(column, value, row));

		// adding and removing at scale yields correct exists value for row and
		// column and value
		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			if(Numbers.isEven(i)) {
				service.add(column, value, row);
			}
			else {
				service.remove(column, value, row);
			}
		}
		assertEquals(service.exists(column, value, row),
				Numbers.isEven(scale) ? false : true);
	}

	@Test
	public void testFetch() throws InterruptedException {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();

		// fetch returns all the values in order
		List<Object> values = Lists.newArrayList();
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Object value = randomObject();
			while (values.contains(value)) {
				value = randomObject();
			}
			values.add(value);
			service.add(column, value, row);
		}
		Collections.reverse(values);
		assertEquals(service.fetch(column, row), Sets.newLinkedHashSet(values));

		// fetch returns correctly after removals happen
		Iterator<Object> it = values.iterator();
		while (it.hasNext()) {
			if(rand.nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(column, value, row);
				it.remove();
			}
			else {
				it.next();
			}
		}
		assertEquals(service.fetch(column, row), Sets.newLinkedHashSet(values));

		// historical fetch
		long timestamp = Time.now();
		Thread.sleep(randomSleep());
		it = values.iterator();
		while (it.hasNext()) {
			if(rand.nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(column, value, row);
			}
			else {
				Object value = randomObject();
				service.add(column, value, row);
			}
		}
		assertEquals(service.fetch(column, timestamp, row),
				Sets.newLinkedHashSet(values));

	}

	@Test
	public void testRemove() {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();
		Object value = randomObject();

		assertFalse(service.remove(column, value, row));
		service.add(column, value, row);
		assertTrue(service.remove(column, value, row));
	}

	@Test
	public void testRevert() throws InterruptedException {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();

		List<Object> values = Lists.newArrayList();
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Object value = randomObject();
			while (values.contains(value)) {
				value = randomObject();
			}
			values.add(value);
			service.add(column, value, row);
		}
		Collections.reverse(values);
		Iterator<Object> it = values.iterator();
		while (it.hasNext()) {
			if(rand.nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(column, value, row);
				it.remove();
			}
			else {
				it.next();
			}
		}
		assertEquals(service.fetch(column, row), Sets.newLinkedHashSet(values));
		long timestamp = Time.now();
		Thread.sleep(randomSleep());
		it = Sets.newLinkedHashSet(values).iterator();

		while (it.hasNext()) {
			if(rand.nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(column, value, row);
			}
			else {
				Object value = randomObject();
				service.add(column, value, row);
			}
		}
		assertTrue(service.revert(column, timestamp, row));
		assertEquals(service.fetch(column, row),
				service.fetch(column, timestamp, row));
	}

	@Test
	public void testQuery() {
		ConcourseService service;
		Object value;
		String column;
		Set<Long> equal;
		Set<Long> notEqual;
		Set<Long> greater;
		Set<Long> less;
		int scale;

		// EQUALS
		service = getService();
		column = randomColumnName();
		value = randomObject();
		equal = Sets.newHashSet();
		notEqual = Sets.newHashSet();

		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) { // add equal values
			long row = randomLong();
			equal.add(row);
			service.add(column, value, row);
		}

		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) { // add not values
			long row = randomLong();
			Object value2 = randomObject();
			while (value.toString().compareTo(value2.toString()) == 0) {
				value2 = randomObject();
			}
			notEqual.add(row);
			service.add(column, value2, row);
		}

		assertEquals(equal, service.query(column, Operator.EQUALS, value));

		// NOT EQUALS
		assertEquals(notEqual,
				service.query(column, Operator.NOT_EQUALS, value));

		// SANITY CHECKS
		service = getService();
		column = randomColumnName();

		long a = randomNegativeLong();
		long rowA = randomLong();

		long b = randomPositiveLong();
		long rowB = randomLong();
		while (rowB == rowA) {
			rowB = randomLong();
		}
		service.add(column, Long.toString(a), rowA);
		service.add(column, b, rowB);
		assertTrue(service.query(column, Operator.GREATER_THAN_OR_EQUALS, a)
				.contains(rowB));
		assertTrue(service.query(column, Operator.GREATER_THAN_OR_EQUALS, a)
				.contains(rowA));

		// GREATER THAN/EQUAL && LESS THAN/EQUAL
		service = getService();
		column = randomColumnName();
		value = randomObject();
		equal = Sets.newHashSet();
		greater = Sets.newHashSet();
		less = Sets.newHashSet();
		scale = randomScaleFrequency();

		for (int i = 0; i < scale; i++) {
			long row = randomLong();
			Object value2 = randomObject();
			int compare;
			if(value instanceof Number && value2 instanceof Number) {
				compare = Numbers.compare((Number) value, (Number) value2);
			}
			else {
				compare = value.toString().compareTo(value2.toString());
			}
			if(compare == 0) { // value == value2
				equal.add(row);
			}
			else if(compare > 0) { // value > value2
				less.add(row);
			}
			else { // value < value2
				greater.add(row);
			}
			service.add(column, value2, row);
		}
		while (equal.isEmpty()) {
			long row = randomLong();
			service.add(column, value, row);
			equal.add(row);
		}

		assertEquals(greater,
				service.query(column, Operator.GREATER_THAN, value));
		Set<Long> gte = Sets.newHashSet();
		gte.addAll(greater);
		gte.addAll(equal);
		assertEquals(gte,
				service.query(column, Operator.GREATER_THAN_OR_EQUALS, value));

		assertEquals(less, service.query(column, Operator.LESS_THAN, value));
		Set<Long> lte = Sets.newHashSet();
		lte.addAll(less);
		lte.addAll(equal);
		assertEquals(lte,
				service.query(column, Operator.LESS_THAN_OR_EQUALS, value));

		assertEquals(equal, service.query(column, Operator.EQUALS, value));

		// TODO REGEX and NOT REGEX

	}

	@Test
	public void testSet() {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();
		int scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			service.add(column, randomObject(), row);
		}
		Object value = randomObject();
		while (service.exists(column, value, row)) {
			value = randomObject();
		}
		assertTrue(service.set(column, value, row));
		assertEquals(1, service.fetch(column, row).size());
		assertTrue(service.fetch(column, row).contains(value));

		// setting an existing value works
		scale = randomScaleFrequency();
		for (int i = 0; i < scale; i++) {
			service.add(column, randomObject(), row);
		}
		assertTrue(service.set(column, value, row));
		assertEquals(1, service.fetch(column, row).size());
		assertTrue(service.fetch(column, row).contains(value));
	}

	@Test
	public void testColumnName() {
		List<String> good = Lists.newArrayList();;
		List<String> bad = Lists.newArrayList();

		good.add(randomColumnName());

		String withNumbers = "";
		int scale = rand.nextInt(7) + 1;
		for (int i = 0; i < scale; i++) {
			withNumbers += randomColumnName().concat(randomNumber().toString());
		}
		good.add(withNumbers);

		Operator operator = Operator.values()[rand
				.nextInt(Operator.values().length)];
		String withOperator = "";
		scale = rand.nextInt(3);
		for (int i = 0; i < scale; i++) {
			withOperator += randomColumnName();
		}
		withOperator += operator;
		scale = rand.nextInt(3);
		for (int i = 0; i < scale; i++) {
			withOperator += randomColumnName();
		}
		bad.add(withOperator);

		String withSpace = "";
		scale = rand.nextInt(5) + 1;
		for (int i = 0; i < scale; i++) {
			withSpace += randomColumnName() + " ";
		}
		bad.add(withSpace.trim());

		CharSequence illegal = ConcourseService.ILLEGAL_COLUMN_NAME_CHARS[rand
				.nextInt(ConcourseService.ILLEGAL_COLUMN_NAME_CHARS.length)];
		String withIllegal = "";
		scale = rand.nextInt(3);
		for (int i = 0; i < scale; i++) {
			withIllegal += randomColumnName();
		}
		withIllegal += illegal;
		scale = rand.nextInt(3);
		for (int i = 0; i < scale; i++) {
			withIllegal += randomColumnName();
		}
		bad.add(withIllegal);

		for (String g : good) {
			assertTrue(ConcourseService.checkColumnName(g));
		}
		for (String b : bad) {
			try {
				ConcourseService.checkColumnName(b);
				log("The bad column name is {}", bad);
				fail();
			}
			catch (IllegalArgumentException e) {
				// pass
			}
		}
	}

	@Test
	public void testReindex() {
		log("Running testReindex");
		ConcourseService service = getService();
		if(IndexingService.class.isAssignableFrom(service.getClass())) {
			int scale = randomScaleFrequency() * 5;

			int pool = Math.round(0.09f * scale);
			log("The column pool is {}", pool);
			String[] columnPool = new String[pool];

			pool = Math.round(0.4f * scale);
			log("The row pool is {}", pool);
			long[] rowPool = new long[pool];

			for (int i = 0; i < rowPool.length; i++) {
				rowPool[i] = randomLong();
			}
			for (int i = 0; i < columnPool.length; i++) {
				columnPool[i] = randomColumnName();
			}

			String[] columns = new String[scale];
			Object[] values = new Object[scale];
			long[] rows = new long[scale];

			// generate data
			log("Generating {} values", scale);
			for (int i = 0; i < scale; i++) {
				columns[i] = columnPool[rand.nextInt(columnPool.length)];
				values[i] = randomObject();
				rows[i] = rowPool[rand.nextInt(rowPool.length)];
			}

			// add data
			log("Adding {} values", scale);
			for (int i = 0; i < scale; i++) {
				service.add(columns[i], values[i], rows[i]);
			}

			scale = Math.round(0.3f * scale);
			// remove data
			log("Removing up to {} values", scale);
			for (int i = 0; i < scale; i++) {
				int index = rand.nextInt(values.length);
				service.remove(columns[index], values[index], rows[index]);
			}

			// perform query before the reindex
			Operator operator = Operator.values()[rand.nextInt(Operator
					.values().length)];
			while (operator == Operator.BETWEEN
					|| operator == Operator.CONTAINS) {
				operator = Operator.values()[rand
						.nextInt(Operator.values().length)];
			}
			String column = columns[rand.nextInt(columns.length)];
			Object value = values[rand.nextInt(values.length)];

			log("Performing query for {} {} {} BEFORE reindex", column,
					operator, value);
			Set<Long> pre = service.query(column, operator, values);
			log("The results of the query were {}", pre);

			// run reindex
			log("Running a reindex");
			((IndexingService) service).reindex();
			log("Performing query for {} {} {} AFTER index", column, operator,
					value);
			Set<Long> post = service.query(column, operator, values);
			log("The results of the query were {}", post);
			assertEquals(pre, post);

		}
	}
}
