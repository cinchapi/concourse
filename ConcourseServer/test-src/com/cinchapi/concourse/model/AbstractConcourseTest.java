package com.cinchapi.concourse.model;

import org.junit.Test;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * Base unit test implementation for classes that extend
 * {@link AbstractConcourse}.
 * 
 * @author jnelson
 * 
 */
public abstract class AbstractConcourseTest extends ConcourseTest {

	/**
	 * Return an instance with random data.
	 * 
	 * @return a populated instance.
	 */
	protected abstract Concourse getPopulatedInstance();

	@Override
	protected final Object getRandomValue() {
		int seed = rand.nextInt();
		if(seed % 5 == 0) {
			return getRandomValueBoolean();
		}
		else if(seed % 2 == 0) {
			return getRandomValueNumber();
		}
		else {
			return getRandomValueString();
		}
	}

	/**
	 * Return a random string.
	 * 
	 * @return a random string.
	 */
	protected String getRandomValueString() {
		return strand.nextStringAllowDigits();
	}

	/**
	 * Retun a random number.
	 * 
	 * @return a random number.
	 */
	protected Number getRandomValueNumber() {
		int seed = rand.nextInt();
		if(seed % 5 == 0) {
			return rand.nextFloat();
		}
		else if(seed % 4 == 0) {
			return rand.nextDouble();
		}
		else if(seed % 3 == 0) {
			return rand.nextLong();
		}
		else {
			return rand.nextInt();
		}
	}

	/**
	 * Return a random boolean.
	 * 
	 * @return a random boolean.
	 */
	protected Boolean getRandomValueBoolean() {
		int seed = rand.nextInt();
		if(seed % 2 == 0) {
			return true;
		}
		else {
			return false;
		}
	}

	@Test
	public void testToString(){
		Concourse concourse = getPopulatedInstance();
		try{
			new JsonParser().parse(concourse.toString());
		}
		catch(JsonParseException e){
			fail("Could not deserialize json from toString() method");
			System.out.println(concourse);
		}
	}
}
