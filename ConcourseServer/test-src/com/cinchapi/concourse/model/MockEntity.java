package com.cinchapi.concourse.model;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.id.IdGenerator;
import com.cinchapi.concourse.model.AbstractEntity;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.MetadataRecord;
import com.cinchapi.concourse.model.api.PropertyRecord;
import com.cinchapi.concourse.property.api.Property;


/**
 * An {@link Entity} used for testing. Does not have any functionality. Only use as a placeholder for 
 * places where an {@link Entity} is needed and not expected to act or be acted upon.
 * @author jnelson
 *
 */
public class MockEntity extends AbstractEntity{
	
	static IdGenerator idgen = new IdGenerator(){
		
		Random random = new Random();

		@Override
		public Id requestId() {
			return new Id(Integer.toString(random.nextInt()));
		}
		
	};

	public MockEntity(String classifier, String title) {
		super(classifier, title);
	}

	@Override
	protected Id createId() {
		return idgen.requestId();
	}

	@Override
	protected MetadataRecord createMetadataRecordInstance(String classifier, String title) {
		return null;
	}

	@Override
	protected Map<String, Set<PropertyRecord<?>>> createEmptyDataMap() {
		return null;
	}

	@Override
	protected PropertyRecord<?> createPropertyRecordInstance(Property<?> property) {
		return null;
	}

	@Override
	protected Set<PropertyRecord<?>> createEmptyPropertyRecordSet() {
		return null;
	}

	@Override
	protected Set<Property<?>> createEmptyPropertySet() {
		return null;
	}
	

}
