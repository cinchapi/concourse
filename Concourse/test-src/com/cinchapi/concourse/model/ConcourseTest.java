package com.cinchapi.concourse.model;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.cinchapi.util.Counter;
import com.cinchapi.util.RandomString;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

import junit.framework.TestCase;

/**
 * Base unit tests for classes that implement the {@link Concourse} interface.
 * 
 * @author jnelson
 * 
 */
public abstract class ConcourseTest extends TestCase {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	protected Counter counter = new Counter();
	protected Random rand = new Random();
	protected RandomString strand = new RandomString();
	
	/**
	 * Get an random {@code count} value for scale tests.
	 * @param maxCountIfLimitScaleTests
	 * @return the {@code count}.
	 */
	protected int count(int maxCountIfLimitScaleTests){
		int count = limitScaleTests() ? rand.nextInt(maxCountIfLimitScaleTests) : rand.nextInt();
		return Math.abs(count);
	}

	/**
	 * Return {@code true} if scale tests should be limited so that they
	 * don't take a long time.
	 * 
	 * @return {@code true} if scale tests should be lmited.
	 */
	protected abstract boolean limitScaleTests();

	/**
	 * Return a new store with no rows.
	 * 
	 * @return an empty instance.
	 */
	protected abstract Concourse getEmptyInstance();

	/**
	 * Return the next row.
	 * 
	 * @return the row.
	 */
	protected UnsignedLong getNextRow() {
		return UnsignedLong.valueOf(counter.next());
	}

	/**
	 * Return a random column.
	 * 
	 * @return the column.
	 */
	protected String getRandomColumn() {
		return strand.nextString().replace(" ", "");
	}

	/**
	 * Return a random value.
	 * 
	 * @return the value.
	 */
	protected abstract Object getRandomValue();

	@Test
	public void testAdd() {
		Concourse concourse = getEmptyInstance();

		UnsignedLong row = getNextRow();
		String column = getRandomColumn();
		Object value = getRandomValue();

		// add works
		assertTrue(concourse.add(row, column, value));
		assertTrue(concourse.exists(row, column, value));

		// cannot add duplicates
		assertFalse(concourse.add(row, column, value));

		// can add multiple values to column in row
		Object value2 = getRandomValue();
		while (value2.equals(value)) {
			value2 = getRandomValue();
		}
		assertTrue(concourse.add(row, column, value2));
		assertTrue(concourse.exists(row, column, value2));
		assertTrue(concourse.exists(row, column, value));
		assertEquals(2, concourse.get(row, column).size());
		int count = limitScaleTests() ? rand.nextInt(20) : rand.nextInt();
		count = Math.abs(count);
		for (int i = 0; i < count; i++) {
			Object _value = getRandomValue();
			while (concourse.exists(row, column, _value)) {
				_value = getRandomValue();
			}
			assertTrue("Problem with adding " + _value,
					concourse.add(row, column, _value));
			assertTrue(concourse.exists(row, column, _value));
		}
		assertEquals(2 + count, concourse.get(row, column).size());

		// can add multiple columns to a row
		String column2 = getRandomColumn();
		while (column2.equals(column)) {
			column2 = getRandomColumn();
		}
		assertTrue(concourse.add(row, column2, value));
		assertTrue(concourse.exists(row, column2, value));
		assertTrue(concourse.exists(row, column));
		assertEquals(2, concourse.describe(row).size());
		count = limitScaleTests() ? rand.nextInt(20) : rand.nextInt();
		count = Math.abs(count);
		for (int i = 0; i < count; i++) {
			String _column = getRandomColumn();
			Object _value = getRandomValue();
			while (concourse.exists(row, _column)) {
				_column = getRandomColumn();
			}
			assertTrue(concourse.add(row, _column, _value));
			assertTrue(concourse.exists(row, _column));

		}
		assertEquals(2 + count, concourse.describe(row).size());

		// can add multiple rows
		UnsignedLong row2 = getNextRow();
		assertTrue(concourse.add(row2, column, value));
		assertTrue(concourse.exists(row2, column, value));
		assertTrue(concourse.exists(row));
		assertEquals(UnsignedLong.valueOf(2), concourse.size());
		count = limitScaleTests() ? rand.nextInt(20) : rand.nextInt();
		count = Math.abs(count);
		for (int i = 0; i < count; i++) {
			UnsignedLong _row = getNextRow();
			String _column = getRandomColumn();
			Object _value = getRandomValue();
			assertTrue(concourse.add(_row, _column, _value));

		}
		assertEquals(UnsignedLong.valueOf(2+count), concourse.size());
	}
	
	@Test
	public void testDescribe(){
		Concourse concourse = getEmptyInstance();
		
		UnsignedLong row = getNextRow();
		String column = getRandomColumn();
		Object value = getRandomValue();
		
		//column isn't in description if not added
		assertFalse(concourse.describe(row).contains(column));
		
		//column is in description if added
		concourse.add(row, column, value);
		assertTrue(concourse.describe(row).contains(column));
		int count = count(10);
		for(int i = 0; i < count; i++){
			String _column = getRandomColumn();
			while(_column.equals(column)){
				_column = getRandomColumn();
			}
			concourse.add(row, _column, getRandomValue());
			assertTrue(concourse.describe(row).contains(_column));
		}
		
		//column isn't in description if removed
		concourse.remove(row, column, value);
		assertFalse(concourse.describe(row).contains(column));
	}
	
	@Test
	public void testExists(){
		Concourse concourse = getEmptyInstance();
		
		//test that non added data does not exist
		UnsignedLong row = getNextRow();
		String column = getRandomColumn();
		Object value = getRandomValue();
		assertFalse(concourse.exists(row));
		assertFalse(concourse.exists(row, column));
		assertFalse(concourse.exists(row, column, value));
		
		//test that added data does exist
		concourse.add(row, column, value);
		assertTrue(concourse.exists(row));
		assertTrue(concourse.exists(row, column));
		assertTrue(concourse.exists(row, column, value));
		
		//test that multiple rows can exist
		UnsignedLong row2 = getNextRow();
		concourse.add(row2, column, value);
		assertTrue(concourse.exists(row));
		assertTrue(concourse.exists(row2));
		int count = count(50);
		for(int i = 0; i < count; i++){
			UnsignedLong _row = getNextRow();
			assertFalse(concourse.exists(_row));
			concourse.add(_row, column, value);
			assertTrue(concourse.exists(_row));
		}
		
		//test that multiple columns can exist in a row
		String column2 = getRandomColumn();
		concourse.add(row, column2, value);
		assertTrue(concourse.exists(row, column));
		assertTrue(concourse.exists(row, column2));
		count = count(50);
		for(int i = 0; i < count; i++){
			String _column = getRandomColumn();
			while(_column.equals(column) || _column.equals(column2)){
				_column = getRandomColumn();
			}
			assertFalse(concourse.exists(row, _column));
			concourse.add(row, _column, value);
			assertTrue(concourse.exists(row, _column));
		}
		
		//test that multiple values can exist in a cell
		Object value2 = getRandomValue();
		concourse.add(row, column, value2);
		assertTrue(concourse.exists(row2, column, value));
		assertTrue(concourse.exists(row, column, value2));
		count = count(50);
		for(int i = 0; i < count; i++){
			Object _value = getRandomValue();
			concourse.add(row, column, _value);
			assertTrue(concourse.exists(row, column, _value));
		}
		
		//removed a value from a cell in a row means that it does not exist
		concourse.remove(row, column2, value);
		assertFalse(concourse.exists(row, column2, value));
		
		//removing all the values in a cell means that the column does not exist in the row
		Iterator<Object> values = concourse.get(row, column).iterator();
		while(values.hasNext()){
			concourse.remove(row, column, values.next());
		}
		assertFalse(concourse.exists(row, column));
		
		//removing all the values from all the columns in a row means that the row does not exist
		List<String> columns = Lists.newArrayList(concourse.describe(row));
		for(int i = 0; i < columns.size(); i++){
			String _column = columns.get(i);
			List<Object> _values = Lists.newArrayList(concourse.get(row, _column));
			for(int j = 0; j < _values.size(); j++){
				Object _value = _values.get(j);
				concourse.remove(row, _column, _value);
			}
		}
		assertFalse(concourse.exists(row));
		
	}
	
	public void testGet(){
		Concourse concourse = getEmptyInstance();
		
		//added data can be retrieved
		UnsignedLong row = getNextRow();
		String column = getRandomColumn();
		Object value = getRandomValue();
		concourse.add(row, column, value);
		assertEquals(1, concourse.get(row, column).size());
		assertTrue(concourse.get(row, column).contains(value));
		
		//removed data cannot be retrieved
		concourse.remove(row, column, value);
		assertEquals(0, concourse.get(row, column).size());
		assertFalse(concourse.get(row, column).contains(value));
	}
	

}
