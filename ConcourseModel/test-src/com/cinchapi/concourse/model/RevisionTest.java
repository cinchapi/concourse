package com.cinchapi.concourse.model;

import java.util.List;
import java.util.Random;

import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import com.cinchapi.commons.util.RandomString;
import com.cinchapi.concourse.model.id.SimpleIDService;
import com.cinchapi.concourse.model.id.IDService;
import com.google.common.collect.Lists;

import junit.framework.TestCase;

public abstract class RevisionTest extends TestCase {
	
	protected static IDService ids = new SimpleIDService();


	/**
	 * Return a random <code>key</code>.
	 * 
	 * @return the <code>key</code>
	 */
	protected String key(){
		return new RandomString().nextString();
	}

	/**
	 * Returns a mock {@link Thing}.
	 * 
	 * @return mock <code>thing</code>.
	 */
	protected Thing thing(){
		Thing mock = Mockito.mock(Thing.class);
		Mockito.when(mock.getId()).thenReturn(ids.requestRandom());
		return mock;
	}

	/**
	 * Returns a revision using random parameters.
	 * 
	 * @return the <code>revision</code>.
	 */
	protected Revision<String> revision(){
		Thing thing = thing();
		String key = key();
		String value = new RandomString().nextString();
		return revision(thing, key, value);
	}

	/**
	 * Returns a revision using the specified parameters, that occurs at the
	 * current timestamp.
	 * 
	 * @param thing
	 * @param key
	 * @param value
	 * @return the <code>revision</code>.
	 */
	protected <T> Revision<T> revision(Thing thing, String key, T value){
		return revision(thing, key, value, DateTime.now());
	}

	/**
	 * Provide a revision using the specified parameters.
	 * 
	 * @param thing
	 * @param key
	 * @param value
	 * @param timestamp
	 * @return the <code>revision</code>.
	 */
	protected abstract <T> Revision<T> revision(Thing thing, String key,
			T value, DateTime timestamp);

	@Test
	public void testPrimaryKeysAreSequential(){
		Revision<String> rev1 = revision();
		Revision<String> rev2 = revision();
		assertTrue(rev1.getPrimaryKey().compareTo(rev2.getPrimaryKey()) < 0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testLocator(){
		Thing thing = thing();
		String key = key();

		//test that different value types produces different locators
		List values = Lists.newArrayList();
		values.add(new Random().nextInt());
		values.add(values.get(0).toString());
		values.add(((Integer) values.get(0)).longValue());
		values.add(((Integer) values.get(0)).doubleValue());
		values.add(((Integer) values.get(0)).floatValue());

		List<Revision<?>> revisions = Lists.newArrayList();
		for(Object value : values){
			revisions.add(revision(thing, key, value));
		}

		for(Revision rev1 : revisions){
			for(Revision rev2 : revisions){
				if(rev1 == rev2){
					continue;
				}
				else{
					System.out.println(rev1.getThingId());
					assertFalse(rev1.getLocator().compareTo(rev2.getLocator()) == 0); 
				}
			}
		}
	}
}
