package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.PropertyRecord;
import com.cinchapi.concourse.property.api.Property;

/**
 * Default implementation of the {@link PropertyRecord} interface.
 * @author jnelson
 *
 * @param <T> - the <code>value</code> type of the <code>property</code>
 */
public class DefaultPropertyRecord<T> extends AbstractPropertyRecord<T>{

	/**
	 * Create a new {@link DefaultPropertyRecord} that associates the <code>entity</code> with the <code>property</code>.
	 * @param entity
	 * @param property
	 */
	public DefaultPropertyRecord(Entity entity, Property<T> property) {
		super(entity, property);
	}
	
	/**
	 * Create a new {@link DefaultPropertyRecord} that associates the <code>entity</code> with the <code>property</code>
	 * at the <coded>added</code> timestamp.
	 * @param entity
	 * @param property
	 * @param added
	 */
	public DefaultPropertyRecord(Entity entity, Property<T> property, DateTime added){
		super(entity, property, added);
	}

}
