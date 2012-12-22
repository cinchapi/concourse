package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.cinchapi.commons.util.Counter;
import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.id.IdGenerator;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Metadata;
import com.cinchapi.concourse.model.api.Modification;
import com.cinchapi.concourse.model.api.Modification.Type;
import com.cinchapi.concourse.property.api.Property;

/**
 * Default implementation of the {@link Entity} interface.
 * @author jnelson
 *
 */
public class DefaultEntity extends AbstractEntity{
	
	protected static final int DEFAULT_DATA_MAP_CAPACITY = 100;
	private static final IdGenerator idgen;

	public DefaultEntity(String classifier, String title) {
		super(classifier, title);
	}

	@Override
	protected Id createId() {
		return idgen.requestId();
	}

	@Override
	protected Set<Property<?>> createEmptyPropertySet() { 
		return new HashSet<Property<?>>(); 
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Modification<?> createModification(Property<?> property, Type type) { 
		return new DefaultModification(this, property, type);
	}

	@Override
	protected Metadata createMetadata(String classifier, String title) {
		return new DefaultMetadata(this, classifier, title);
	}

	@Override
	protected Map<String, Modification<?>> createEmptyModifications() {
		return new HashMap<String, Modification<?>>();
	}

	@Override
	protected Map<String, Set<Property<?>>> createEmptyData() {
		return new HashMap<String, Set<Property<?>>>();
	}
	
	static{
		idgen = new IdGenerator(){
			
			Counter counter = new Counter();

			@Override
			public Id requestId() {
				return new Id(Long.toString(counter.next()));
			}
			
		};
	}

}
