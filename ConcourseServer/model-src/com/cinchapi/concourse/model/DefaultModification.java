package com.cinchapi.concourse.model;

import java.util.ArrayList;
import java.util.List;

import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.property.api.Property;

/**
 * Default implementation of the {@link Modification} interface.
 * @author jnelson
 *
 * @param <T> - the {@link Property} type
 */
public class DefaultModification<T> extends AbstractModification<T>{

	/**
	 * Create a new {@link DefaultModification}.
	 * @param entity
	 * @param property
	 * @param type
	 */
	public DefaultModification(Entity entity, Property<T> property, Type type) {
		super(entity, property, type);
	}
	
	@Override 
	public List<String> asList(){
		ArrayList<String> list = new ArrayList<String>();
		list.add(getLookup());
		list.add(getEntity().getId().toString());
		list.add(getProperty().getKey());
		list.add(getProperty().getValue().toString());
		list.add(getProperty().getType());
		list.add(getType().toString());
		list.add(getTimestamp().toString());
		return list;
	}
}
