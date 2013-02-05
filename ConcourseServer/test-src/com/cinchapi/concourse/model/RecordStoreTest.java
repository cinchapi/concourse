package com.cinchapi.concourse.model;

import java.util.Random;

import org.junit.Test;
import org.mockito.Mockito;

import com.cinchapi.util.Counter;
import com.cinchapi.util.RandomString;
import com.google.common.primitives.UnsignedLong;

import junit.framework.TestCase;

/**
 * Unit tests for the contract specified in {@link Concourse}.
 * 
 * @author jnelson
 */
public abstract class RecordStoreTest extends TestCase {

	private Counter counter = new Counter();
	private Random rand = new Random();
	private RandomString strand = new RandomString();

	/**
	 * Return a random value.
	 * 
	 * @return the value.
	 */
	protected abstract Object getRandomValue();

	/**
	 * Return the next record id.
	 * 
	 * @return the id.
	 */
	protected UnsignedLong getNextId(){
		return UnsignedLong.valueOf(counter.next());
	}

	/**
	 * Return a store with no records.
	 * 
	 * @return the store
	 */
	protected Concourse getEmptyStore(){
		return Mockito.mock(Concourse.class);
	}

	/**
	 * Return a store that contains the specified list of records
	 * 
	 * @param records
	 * @return the store
	 */
	protected Concourse getStoreContainingRecords(RecordContainer...records){
		Concourse store = getEmptyStore();
		for(int i = 0; i < records.length; i++){
			RecordContainer record = records[i];
			store.add(record.id, record.Column, record.value);
		}
		return store;
	}

	/**
	 * Return a random Column.
	 * 
	 * @return the Column
	 */
	protected String getRandomColumn(){
		return strand.nextString();
	}

	@Test
	public void testAdd(){
		Concourse store = getEmptyStore();

		UnsignedLong id = getNextId();
		String Column = getRandomColumn();
		Object value = getRandomValue();

		assertTrue(store.add(id, Column, value));
		assertTrue(store.exists(id, Column, value));
		assertFalse(store.add(id, Column, value)); // cannot add an existing
													// Column/value pair
		assertEquals(1, store.get(id, Column).size());
		assertTrue(store.get(id, Column).contains(value));

		Object value2 = getRandomValue();

		assertFalse(value.equals(value2)); // make sure that values are
											// different
		assertTrue(store.add(id, Column, value2));
		assertTrue(store.exists(id, Column, value)); // make sure first value
														// is not overwritten
		assertTrue(store.exists(id, Column, value2));
		assertEquals(2, store.get(id, Column).size());
		assertTrue(store.get(id, Column).contains(value2));

		int count = Math.abs(rand.nextInt()); // scale test the number of values
												// for a single Column
		for(int i = 0; i < count; i++){
			Object _value = getRandomValue();
			while (store.exists(id, Column, _value)){
				_value = getRandomValue();
			}
			assertTrue(store.add(id, Column, _value));
			assertTrue(store.exists(id, Column, _value));
		}
		assertEquals(2 + count, store.get(id, Column).size());

		String Column2 = getRandomColumn();

		assertTrue(store.add(id, Column2, value)); // check that it is possible
													// to add multiple Columns
		assertTrue(store.exists(id, Column));
		assertTrue(store.exists(id, Column2));

		count = Math.abs(rand.nextInt()); // scale test the number of Columns for
											// a single record
		for(int i = 0; i < count; i++){
			String _Column = getRandomColumn();
			Object _value = getRandomValue();
			while (store.exists(id, _Column)){
				_Column = getRandomColumn();
			}
			assertTrue(store.add(id, _Column, _value));
			assertTrue(store.exists(id, _Column));

		}
		assertEquals(2 + count, store.describe(id).size());

		UnsignedLong id2 = getNextId();
		assertTrue(store.add(id2, Column, value)); // check that it is possible
													// to add multiple records
		assertTrue(store.exists(id2, Column, value));
		assertTrue(store.exists(id, Column, value)); // check that the presence
														// of multiple records
														// with same Column/value
														// pair is possible

		count = (int) Math.abs(rand.nextLong()); // scale test the number of
													// records for a single
													// store
		for(int i = 0; i < count; i++){
			UnsignedLong _id = getNextId();
			String _Column = getRandomColumn();
			Object _value = getRandomValue();
			assertTrue(store.add(_id, _Column, _value));
			assertTrue(store.exists(_id, _Column, _value));
		}
		assertEquals(2 + count, store.size());
	}

	@Test
	public void testContains(){
		UnsignedLong id = getNextId();
		String Column = getRandomColumn();
		Object value = getRandomValue();

		UnsignedLong id2 = getNextId();
		String Column2 = getRandomColumn();
		Object value2 = getRandomValue();

		Concourse store = getStoreContainingRecords(RecordContainer.create(
				id, Column, value));

		assertTrue(store.exists(id, Column, value));
		assertFalse(store.exists(id2, Column2, value2));
		assertFalse(store.exists(id2, Column2, value));
		assertFalse(store.exists(id2, Column, value));
		assertFalse(store.exists(id2, Column, value2));
		assertFalse(store.exists(id, Column, value2));
		assertFalse(store.exists(id, Column2, value2));
		assertFalse(store.exists(id, Column2, value));

		assertTrue(store.remove(id, Column, value));
		assertFalse(store.exists(id, Column, value));
		
		//TODO test removal cases
	}

	@Test
	public void testhasColumn(){
		UnsignedLong id = getNextId();
		String Column = getRandomColumn();
		Object value = getRandomValue();

		UnsignedLong id2 = getNextId();
		String Column2 = getRandomColumn();
		Object value2 = getRandomValue();

		Concourse store = getStoreContainingRecords(
				RecordContainer.create(id, Column, value),
				RecordContainer.create(id2, Column2, value2));

		assertTrue(store.exists(id, Column));
		assertFalse(store.exists(id2, Column));
		assertTrue(store.exists(id2, Column2));
		assertFalse(store.exists(id, Column2));
		assertFalse(store.exists(getNextId(), getRandomColumn()));
		
		assertTrue(store.remove(id, Column, value)); 
		assertFalse(store.exists(id, Column)); //check that removing Column with one value works
		assertTrue(store.add(id, Column, value)); 
		assertFalse(store.exists(id, Column));//check that a reoved Column can be readded
		assertTrue(store.add(id, Column, value2));
		assertTrue(store.remove(id, Column, value));
		assertTrue(store.exists(id, Column)); //check that removing a Column with multiple values works
	}

	@Test
	public void testExists(){
		UnsignedLong id = getNextId();
		UnsignedLong id2 = getNextId();

		Concourse store = getStoreContainingRecords(
				RecordContainer.create(id, getRandomColumn(), getRandomValue()),
				RecordContainer.create(id2, getRandomColumn(), getRandomValue()));
		
		assertTrue(store.exists(id));
		assertTrue(store.exists(id));
		UnsignedLong id3 = getNextId();
		assertFalse(store.exists(id3));
		
		store.add(id3, getRandomColumn(), getRandomValue());
		assertTrue(store.exists(id3));
		
		
		
		
	}

	/**
	 * A record conainer
	 */
	public static class RecordContainer {

		UnsignedLong id;
		String Column;
		Object value;

		public RecordContainer(UnsignedLong id, String Column, Object value) {
			this.id = id;
			this.Column = Column;
			this.value = value;
		}

		/**
		 * Create a new record container
		 * 
		 * @param id
		 * @param Column
		 * @param value
		 * @return the container
		 */
		public static RecordContainer create(UnsignedLong id, String Column,
				Object value){
			return new RecordContainer(id, Column, value);
		}
	}

}
