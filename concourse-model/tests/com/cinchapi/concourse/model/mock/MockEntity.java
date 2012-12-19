package com.cinchapi.concourse.model.mock;

import java.util.Iterator;
import java.util.Set;

import com.cinchapi.concourse.data.Property;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.MetadataRecord;
import com.cinchapi.concourse.model.PropertyRecord;


/**
 * An {@link Entity} used for testing. Does not have any functionality. Only use as a placeholder for 
 * places where an {@link Entity} is needed and not expected to act or be acted upon.
 * @author jnelson
 *
 */
@SuppressWarnings("rawtypes")
public class MockEntity implements Entity{
	
	MockLongId id;
	
	private static final MockIdGenerator idgen = new MockIdGenerator();
	
	public MockEntity(String classifier, String title){
		this.id = idgen.requestId();
	}

	
	@Override
	public MockLongId getId() {
		return id;
	}

	
	@Override
	public  PropertyRecord add(Property property) {
		return null;
	}

	@Override
	public boolean setTitle(String title) {
		return false;
	}

	@Override
	public  boolean contains(Property property) {
		return false;
	}

	@Override
	public  Set<Property> get(String key) {
		return null;
	}

	@Override
	public  PropertyRecord remove(Property property) {
		return null;
	}

	@Override
	public Iterator<String> iterator() {
		return null;
	}

	@Override
	public MetadataRecord getMetadata() {
		return null;
	}

}
