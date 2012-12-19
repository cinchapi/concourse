package com.cinchapi.concourse.model;

import com.cinchapi.concourse.data.Property;

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

}
