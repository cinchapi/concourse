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
package com.cinchapi.concourse.services;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.BaseTest;
import com.cinchapi.concourse.service.ConcourseService;
import com.cinchapi.concourse.service.QueryableService.Operator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@AbstractConcourseService}.
 * 
 * @author jnelson
 */
public abstract class ConcourseServiceTest extends BaseTest {

	protected abstract ConcourseService getService();

	@Test
	public void testAdd() {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();
		Object value = randomObject();
		assertTrue(service.add(row, column, value));

		// can't add dupes
		assertFalse(service.add(row, column, value));

		// multiple values in column
		int scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Object v = randomObject();
			while (service.exists(row, column, v)) {
				v = randomObject();
			}
			assertTrue(service.add(row, column, v));
		}

		// multiple columns in a row
		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			String c = randomColumnName();
			while (service.exists(row, c)) {
				c = randomColumnName();
			}
			assertTrue(service.add(row, c, value));
		}

		// multiple rows
		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			long r = randomLong();
			while (service.exists(r)) {
				r = randomLong();
			}
			assertTrue(service.add(r, column, value));
		}

	}

	@Test
	public void testDescribe() {
		ConcourseService service = getService();

		long row = randomLong();

		int scale = getScaleFrequency();
		Set<String> columns = Sets.newTreeSet();
		for (int i = 0; i < scale; i++) {
			String column = randomColumnName();
			while (columns.contains(column)) {
				column = randomColumnName();
			}
			columns.add(column);
			service.add(row, column, randomObject());
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
		service.add(row, column, value);
		assertTrue(service.exists(row));

		// row doesn't exists after being removed
		service.remove(row, column, value);
		assertFalse(service.exists(row));

		// adding and removing at scale yields correct exists value for row
		int scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			if(Numbers.isEven(i)) {
				service.add(row, column, value);
			}
			else {
				service.remove(row, column, value);
			}
		}
		assertEquals(service.exists(row), Numbers.isEven(scale) ? false : true);

		// row and column exist
		row = randomLong();
		column = randomColumnName();
		value = randomObject();

		assertFalse(service.exists(row, column));
		service.add(row, column, value);
		assertTrue(service.exists(row, column));

		// row and column don't exist after being removed
		service.remove(row, column, value);
		assertFalse(service.exists(row, column));
		service.add(row, randomColumnName(), value);
		assertFalse(service.exists(row, column));

		// adding and removing at scale yields correct exists value for row and
		// column
		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			if(Numbers.isEven(i)) {
				service.add(row, column, value);
			}
			else {
				service.remove(row, column, value);
			}
		}
		assertEquals(service.exists(row, column), Numbers.isEven(scale) ? false
				: true);

		// row and column and value exist
		row = randomLong();
		column = randomColumnName();
		value = randomObject();

		assertFalse(service.exists(row, column, value));
		service.add(row, column, value);
		assertTrue(service.exists(row, column, value));

		// row and column and value don't exist after being removed
		service.remove(row, column, value);
		assertFalse(service.exists(row, column, value));

		// adding and removing at scale yields correct exists value for row and
		// column and value
		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			if(Numbers.isEven(i)) {
				service.add(row, column, value);
			}
			else {
				service.remove(row, column, value);
			}
		}
		assertEquals(service.exists(row, column, value),
				Numbers.isEven(scale) ? false : true);
	}

	@Test
	public void testFetch() throws InterruptedException {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();

		// fetch returns all the values in order
		List<Object> values = Lists.newArrayList();
		int scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Object value = randomObject();
			while (values.contains(value)) {
				value = randomObject();
			}
			values.add(value);
			service.add(row, column, value);
		}
		Collections.reverse(values);
		assertEquals(service.fetch(row, column), Sets.newLinkedHashSet(values));

		// fetch returns correctly after removals happen
		Iterator<Object> it = values.iterator();
		while (it.hasNext()) {
			if(getRandom().nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(row, column, value);
				it.remove();
			}
			else{
				it.next();
			}
		}
		assertEquals(service.fetch(row, column), Sets.newLinkedHashSet(values));

		// historical fetch
		long timestamp = Time.now();
		Thread.sleep(randomSleep());
		it = values.iterator();
		while (it.hasNext()) {
			if(getRandom().nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(row, column, value);
			}
			else {
				Object value = randomObject();
				service.add(row, column, value);
			}
		}
		assertEquals(service.fetch(row, column, timestamp),
				Sets.newLinkedHashSet(values));

	}

	@Test
	public void testRemove() {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();
		Object value = randomObject();

		assertFalse(service.remove(row, column, value));
		service.add(row, column, value);
		assertTrue(service.remove(row, column, value));
	}

	@Test
	public void testRevert() throws InterruptedException {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();

		List<Object> values = Lists.newArrayList();
		int scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			Object value = randomObject();
			while (values.contains(value)) {
				value = randomObject();
			}
			values.add(value);
			service.add(row, column, value);
		}
		Collections.reverse(values);
		Iterator<Object> it = values.iterator();
		while (it.hasNext()) {
			if(getRandom().nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(row, column, value);
				it.remove();
			}
			else{
				it.next();
			}
		}
		assertEquals(service.fetch(row, column), Sets.newLinkedHashSet(values));
		long timestamp = Time.now();
		Thread.sleep(randomSleep());
		it = Sets.newLinkedHashSet(values).iterator();

		while (it.hasNext()) {
			if(getRandom().nextInt() % 3 == 0) {
				Object value = it.next();
				service.remove(row, column, value);
			}
			else {
				Object value = randomObject();
				service.add(row, column, value);
			}
		}
		assertTrue(service.revert(row, column, timestamp));
		assertEquals(service.fetch(row, column),
				service.fetch(row, column, timestamp));
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

		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) { // add equal values
			long row = randomLong();
			equal.add(row);
			service.add(row, column, value);
		}

		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) { // add not values
			long row = randomLong();
			Object value2 = randomObject();
			while (value.toString().compareTo(value2.toString()) == 0) {
				value2 = randomObject();
			}
			notEqual.add(row);
			service.add(row, column, value2);
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
		service.add(rowA, column, Long.toString(a));
		service.add(rowB, column, b);
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
		scale = getScaleFrequency();

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
			service.add(row, column, value2);
		}
		while (equal.isEmpty()) {
			long row = randomLong();
			service.add(row, column, value);
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

		
	}

	@Test
	public void testSet() {
		ConcourseService service = getService();

		long row = randomLong();
		String column = randomColumnName();
		int scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			service.add(row, column, randomObject());
		}
		Object value = randomObject();
		while (service.exists(row, column, value)) {
			value = randomObject();
		}
		assertTrue(service.set(row, column, value));
		assertEquals(1, service.fetch(row, column).size());
		assertTrue(service.fetch(row, column).contains(value));

		// setting an existing value works
		scale = getScaleFrequency();
		for (int i = 0; i < scale; i++) {
			service.add(row, column, randomObject());
		}
		assertTrue(service.set(row, column, value));
		assertEquals(1, service.fetch(row, column).size());
		assertTrue(service.fetch(row, column).contains(value));
	}
	
	@Test
	public void testColumnName(){
		List<String> good = Lists.newArrayList();;
		List<String> bad = Lists.newArrayList();
		
		good.add(randomColumnName());
		
		String withNumbers = "";
		int scale = getRandom().nextInt(7)+1;
		for(int i = 0 ; i < scale; i++){
			withNumbers+= randomColumnName().concat(randomNumber().toString());
		}
		good.add(withNumbers);
		
		Operator operator = Operator.values()[getRandom().nextInt(Operator.values().length)];
		String withOperator = "";
		scale = getRandom().nextInt(3);
		for(int i = 0; i < scale; i++){
			withOperator+= randomColumnName();
		}
		withOperator+=operator;
		scale = getRandom().nextInt(3);
		for(int i = 0; i < scale; i++){
			withOperator+= randomColumnName();
		}
		bad.add(withOperator);
		
		String withSpace = "";
		scale = getRandom().nextInt(5)+1;
		for(int i = 0; i < scale; i++){
			withSpace+= randomColumnName() + " ";
		}
		bad.add(withSpace.trim());
		
		CharSequence illegal = ConcourseService.ILLEGAL_COLUMN_NAME_CHARS[getRandom().nextInt(ConcourseService.ILLEGAL_COLUMN_NAME_CHARS.length)];
		String withIllegal = "";
		scale = getRandom().nextInt(3);
		for(int i = 0; i < scale; i++){
			withIllegal+= randomColumnName();
		}
		withIllegal+=illegal;
		scale = getRandom().nextInt(3);
		for(int i = 0; i < scale; i++){
			withIllegal+= randomColumnName();
		}
		bad.add(withIllegal);
		
		for(String g : good){
			assertTrue(ConcourseService.checkColumnName(g));
		}
		for(String b : bad){
			try{
				ConcourseService.checkColumnName(b);
				fail();
			}
			catch(IllegalArgumentException e){
				//pass
			}
		}
	}

}
