package com.cinchapi.concourse.model;

import org.joda.time.DateTime;
import org.junit.Test;

import com.cinchapi.concourse.data.Property;


import junit.framework.TestCase;

/**
 * Tests for the contract of {@link PropertyRecord} and implementation of {@link AbstractPropertyRecord}.
 * @author jnelson
 *
 */
abstract public class PropertyRecordTest<T> extends TestCase{
	
	/**
	 * Get a copy of the specified {@link PropertyRecord}
	 * @param record
	 * @return a copy
	 */
	public abstract PropertyRecord<T> copy(PropertyRecord<T> record);
	
	/**
	 * Get a new {@link PropertyRecord} instance.
	 * @return new {@link PropertyRecord}
	 */
	public PropertyRecord<T> getInstance(){
		Entity entity = getEntityInstance();
		Property<T> property = getPropertyInstance();
		DateTime added = DateTime.now();
		return getInstance(entity, property, added);
	}
	
	/**
	 * Get a new {@link PropertyRecord} instance with the specified values.
	 * @param entity
	 * @param property
	 * @param added
	 * @return new {@link PropertyRecord} that contains the specified values
	 */
	public abstract PropertyRecord<T> getInstance(Entity entity, Property<T> property, DateTime added);
	
	/**
	 * Get a new {@link PropertyRecord} instance that is marked as removed.
	 * @return new {@link PropertyRecord} that is marked as removed
	 */
	public PropertyRecord<T> getInstanceRemoved(){
		Entity entity = getEntityInstance();
		Property<T> property = getPropertyInstance();
		DateTime added = DateTime.now();
		return getInstanceRemoved(entity, property, added);
	}
	
	/**
	 * Get a new {@link PropertyRecord} instance with the specified values that is marked as removed.
	 * @param entity
	 * @param property
	 * @param added
	 * @return new {@link PropertyRecord} that contains the specified values and is marked as removed 
	 */
	public abstract PropertyRecord<T> getInstanceRemoved(Entity entity, Property<T> property, DateTime added);
	
	/**
	 * Get an {@link Entity} instance
	 * @return new {@link Entity}
	 */
	public abstract Entity getEntityInstance();
	
	/**
	 * Get a {@link Property} instance
	 * @return new {@link Property}
	 */
	public abstract Property<T> getPropertyInstance();
	
	/**
	 * Get a {@link Property} containing the specified <code>key</code> and <code>value</code>.
	 * @param key
	 * @param value
	 * @return new {@link Property}
	 */
	public abstract Property<T> getPropertyInstance(String key, String value);
	
	/**
	 * Sleep for the specified number of milliseconds
	 * @param millis
	 */
	protected void sleep(long millis){
		try {
			Thread.sleep(1);
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetAddedTimestamp(){
		DateTime added = DateTime.now();
		PropertyRecord<T> record = getInstance(getEntityInstance(), getPropertyInstance(), added);
		assertEquals(added, record.getAddedTime());
	}
	
	@Test
	public void testGetEntity(){
		Entity entity = getEntityInstance();
		PropertyRecord<T> record = getInstance(entity, getPropertyInstance(), DateTime.now());
		
		assertEquals(entity, record.getEntity());
	}
	
	@Test
	public void testGetProperty(){
		Property<T> property = getPropertyInstance();
		PropertyRecord<T> record = getInstance(getEntityInstance(), property, DateTime.now());
		
		assertEquals(property, record.getProperty());
	}
	
	@Test
	public void testGetRemovedTimestamp(){
		PropertyRecord<T> record = getInstance();
		DateTime removed = record.markAsRemoved();
		
		assertEquals(removed, record.getRemovedTime());
	}
	
	@Test
	public void testIsRemoved(){
		PropertyRecord<T> recordA = getInstanceRemoved();
		PropertyRecord<T> recordB = getInstance();
		
		assertTrue(recordA.isMarkAsRemoved());
		assertFalse(recordB.isMarkAsRemoved());
	}
	
	@Test
	public void testMarkAsRemoved(){
		PropertyRecord<T> record = getInstance();
		
		assertFalse(record.isMarkAsRemoved());
		
		sleep(1);
		DateTime removed = record.markAsRemoved();
		
		assertTrue(removed.isAfter(record.getAddedTime()));
		
		sleep(1);
		DateTime now = DateTime.now();
		
		assertTrue(removed.isBefore(now));
		assertTrue(record.isMarkAsRemoved());
		assertEquals(removed, record.markAsRemoved());
	}
	
	@Test
	public void testEqualsAndHashCode(){
		//format: [entity][key][value][created]
		Entity entityA = getEntityInstance();
		Entity entityB = getEntityInstance();
		
		String keyA = "Key A";
		String valueA = "Value A";
		DateTime createdA = DateTime.now();
		
		
		String keyB = "Key B";
		String valueB = "Value B";
		
		sleep(1);
		DateTime createdB = DateTime.now();
		
		PropertyRecord<T> aaaa = getInstance(entityA, getPropertyInstance(keyA, valueA), createdA);
		PropertyRecord<T> aaab = getInstance(entityA, getPropertyInstance(keyA, valueA), createdB); 
		assertTrue(aaaa.equals(aaaa)); //identity
		assertEquals(aaaa.hashCode(), aaaa.hashCode());
		assertTrue(aaaa.equals(aaab)); //neither removed, same entity and same property, diff created 
		assertEquals(aaaa.hashCode(), aaab.hashCode());
		sleep(1);
		aaab.markAsRemoved();
		assertFalse(aaaa.equals(aaab)); //one removed, same entity, same property, diff created
		
		PropertyRecord<T> baaa = getInstance(entityB, getPropertyInstance(keyA, valueA), createdA); 
		assertFalse(aaaa.equals(baaa)); //diff entity, same property, same created
		
		PropertyRecord<T> aaba = getInstance(entityA, getPropertyInstance(keyA, valueB), createdA);
		assertFalse(aaaa.equals(aaba)); //same entity, diff property(value), same created
		sleep(1);
		aaba.markAsRemoved();
		assertFalse(aaaa.equals(aaba)); //one removed, same entity diff property(value), same created
		
		PropertyRecord<T> abaa = getInstance(entityA, getPropertyInstance(keyB, valueA), createdA);
		assertFalse(aaaa.equals(abaa)); //same entity, diff property(key), same created
		sleep(1);
		abaa.markAsRemoved();
		assertFalse(aaaa.equals(abaa)); //one removed, same entity diff property(key), same created
		
		sleep(1);
		aaaa.markAsRemoved();
		assertFalse(aaaa.equals(aaab)); //both removed, same entity, same property, diff created
		
		PropertyRecord<T> aaaa2 = getInstance(entityA, getPropertyInstance(keyA, valueA), createdA);
		sleep(1);
		aaaa2.markAsRemoved();
		
		assertFalse(aaaa.equals(aaaa2)); //both removed, same entity, same property, same created, diff removed
		assertTrue(aaaa.equals(copy(aaaa))); //same entity, same property, same created, same removed
		assertEquals(aaaa.hashCode(), aaaa.hashCode());
	}
	
	//TODO test hashCode

}
