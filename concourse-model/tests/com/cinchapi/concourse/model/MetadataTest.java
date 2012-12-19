package com.cinchapi.concourse.model;

import org.junit.Test;

import com.cinchapi.commons.util.RandomString;

import junit.framework.TestCase;

/**
 * Tests for the contract of {@link MetadataRecord}.
 * @author jnelson
 *
 */
public abstract class MetadataTest extends TestCase{
	
	public static RandomString random = new RandomString();
	
	public MetadataRecord getInstance(){
		String classifier = random.nextString();
		String title = random.nextString();
		Entity entity = getEntityInstance(classifier, title);
		return getInstance(entity);
	}
	
	public abstract MetadataRecord getInstance(Entity entity);
	
	public abstract Entity getEntityInstance(String classifier, String title);
	
	@Test
	public void testGet(){
		
	}

}
