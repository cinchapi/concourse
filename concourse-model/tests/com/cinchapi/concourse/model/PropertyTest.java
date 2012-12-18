package com.cinchapi.concourse.model;

import org.junit.Test;

import com.cinchapi.commons.util.RandomString;

import junit.framework.TestCase;

/**
 * Tests for the contract of {@link Property}.
 * @author jnelson
 *
 * @param <T> - the data type of the {@link Property} value
 */
public abstract class PropertyTest<T> extends TestCase{
	
	/**
	 * Get a {@link Property} instance that corresponds to the specified <code>key</code> and <code>value</code>
	 * @return instance
	 */
	public abstract Property<T> getInstance(String key, T value);
	
	/**
	 * Get a random <code>value</code> to use with {@link PropertyTest#getInstance}
	 * @return a random <code>value</code>
	 */
	public abstract T getRandomValue();
	
	/**
	 * Get a random <code>key</code> to use with {@link PropertyTest#getInstance}
	 * @return a random <code>key</code>
	 */
	String getRandomKey(){
		RandomString random = new RandomString();
		return random.nextString();
	}
	
	@Test
	public void testGetKey(){
		String key = getRandomKey().replace(" ", "");
		Property<T> property = getInstance(key, getRandomValue());
		assertEquals(key, property.getKey());
		
		//test key casing
		property = getInstance(key.toUpperCase(), getRandomValue());
		assertEquals(key, property.getKey());
		
		//test key spaces
		while(!key.contains(" ")){
			key = getRandomKey();
		}
		property = getInstance(key, getRandomValue());
		assertFalse(property.getKey().contains(" "));
	}
	
	@Test
	public void testGetValue(){
		T value = getRandomValue();
		Property<T> property = getInstance(getRandomKey(), value);
		assertEquals(value, property.getValue());
	}
	
	@Test
	public void testGetType(){
		Property<T> property = getInstance(getRandomKey(), getRandomValue());
		assertNotNull(property.getValue()); //make sure subclass defines type using {@link com.cinchapi.concourse.annotations.DataType}
	}
	
	@Test
	public void testEqualsAndHashCode(){
		String keyA = getRandomKey();
		String keyB = getRandomKey();
		while(keyA.equalsIgnoreCase(keyB)){
			keyB = getRandomKey();
		}
		
		T valueA = getRandomValue();
		T valueB = getRandomValue();
		while (valueA.equals(valueB)){
			valueB = getRandomValue();
		}
		
		Property<T> aa1 = getInstance(keyA, valueA);
		Property<T> aa2 = getInstance(keyA, valueA);
		
		Property<T> bb = getInstance(keyB, valueB);
		
		Property<T> ab = getInstance(keyA, valueB);
		Property<T> ba = getInstance(keyB, valueA);
		
		assertTrue(aa1.equals(aa2)); //same key same value		
		assertTrue(aa2.equals(aa1)); //same key same value, reflective
		assertEquals(aa1.hashCode(), aa2.hashCode());
		
		assertTrue(bb.equals(bb)); //identity, reflective
		assertEquals(bb.hashCode(), bb.hashCode());
		
		assertFalse(aa1.equals(bb)); //diff key diff value
		assertFalse(bb.equals(aa1)); //diff key diff value, reflective
		
		assertFalse(aa1.equals(ba)); //diff key save value
		assertFalse(aa1.equals(ab)); //same key diff value

	}

}
