package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.cinchapi.commons.util.Counter;
import com.cinchapi.concourse.data.Property;
import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.id.IdGenerator;

/**
 * Default implementation of the {@link Entity} interface.
 * @author jnelson
 *
 */
public class DefaultEntity extends AbstractEntity{
	
	protected static final int DEFAULT_DATA_MAP_CAPACITY = 100;
	private static IdGenerator idgen;
	static{
		idgen = new IdGenerator(){
			
			Counter counter = new Counter();

			@Override
			public Id requestId() {
				return new Id(Long.toString(counter.next()));
			}
			
		};
	}

	@Override
	protected Id createId() {
		return idgen.requestId();
	}

	@Override
	protected MetadataRecord createMetadata(String classifier, String title) {
		return new DefaultMetadataRecord(this, classifier, title);
	}

	@Override
	protected Map<String, Set<PropertyRecord<?>>> createData() {
		return new HashMap<String, Set<PropertyRecord<?>>>(DEFAULT_DATA_MAP_CAPACITY);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected PropertyRecord<?> createPropertyRecord(Property<?> property) {
		return new DefaultPropertyRecord(this, property);
	}

	@Override
	protected Set<PropertyRecord<?>> createEmptyPropertyRecordSet() {
		return new HashSet<PropertyRecord<?>>();
	}

	@Override
	protected Set<Property<?>> createEmptyPropertySet() {
		return new HashSet<Property<?>>();
	}

}
