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
package com.cinahpi.concourse;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.concourse.api.ConcourseService;
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
		String column = randomStringNoSpaces();
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
			String c = randomStringNoSpaces();
			while (service.exists(row, c)) {
				c = randomStringNoSpaces();
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
			String column = randomStringNoSpaces();
			while (columns.contains(column)) {
				column = randomStringNoSpaces();
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
		String column = randomStringNoSpaces();
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
		column = randomStringNoSpaces();
		value = randomObject();

		assertFalse(service.exists(row, column));
		service.add(row, column, value);
		assertTrue(service.exists(row, column));

		// row and column don't exist after being removed
		service.remove(row, column, value);
		assertFalse(service.exists(row, column));
		service.add(row, randomStringNoSpaces(), value);
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
		column = randomStringNoSpaces();
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
	public void testGet(){
		ConcourseService service = getService();
		
		long row = randomLong();
		String column = randomStringNoSpaces();
		
		//get returns all the values in order
		List<Object> values = Lists.newArrayList();
		int scale = getScaleFrequency();
		for(int i = 0; i < scale; i++){
			Object value = randomObject();
			while(values.contains(value)){
				value = randomObject();
			}
			values.add(value);
			service.add(row, column, value);
		}
		Collections.reverse(values);
		assertEquals(service.get(row, column), Sets.newLinkedHashSet(values));
		
		//get returns correctly after removals happen
		Iterator<Object> it = values.iterator();
		while(it.hasNext()){
			if(getRandom().nextInt() %3 == 0){
				Object value = it.next();
				service.remove(row, column, value);
				it.remove();
			}
		}
		assertEquals(service.get(row, column), Sets.newLinkedHashSet(values));
	}
	
	@Test
	public void testRemove(){
		ConcourseService service = getService();
		
		long row = randomLong();
		String column = randomStringNoSpaces();
		Object value = randomObject();
		
		assertFalse(service.remove(row, column, value));
		service.add(row, column, value);
		assertTrue(service.remove(row, column, value));
	}
	
	@Test
	public void testSelect(){
		//TODO IMPLEMENT ME
	}
	
	@Test
	public void testSet(){
		ConcourseService service = getService();
		
		long row = randomLong();
		String column = randomStringNoSpaces();
		int scale = getScaleFrequency();
		for(int i = 0; i < scale; i++){
			service.add(row, column, randomObject());
		}
		Object value = randomObject();
		while(service.exists(row, column, value)){
			value = randomObject();
		}
		assertTrue(service.set(row, column, value));
		assertEquals(1, service.get(row, column).size());
		assertTrue(service.get(row, column).contains(value));
		
		//setting an existing value works 
		scale = getScaleFrequency();
		for(int i = 0; i < scale; i++){
			service.add(row, column, randomObject());
		}
		assertTrue(service.set(row, column, value));
		assertEquals(1, service.get(row, column).size());
		assertTrue(service.get(row, column).contains(value));
	}

	protected String randomStringNoSpaces() {
		return randomString().replace(" ", "");
	}

}
