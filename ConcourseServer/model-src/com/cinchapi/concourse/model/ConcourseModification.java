package com.cinchapi.concourse.model;

import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.property.api.Property;

/**
 * A {@link Modification} for Concourse.
 * @author jnelson
 *
 * @param <T> - the {@link Property} type
 */
@Immutable
public final class ConcourseModification<T> extends AbstractModification<T>{

	/**
	 * Create a new {@link ConcourseModification}.
	 * @param entity
	 * @param property
	 * @param type
	 */
	public ConcourseModification(Entity entity, Property<T> property, Type type) {
		super(entity, property, type);
	}
	
}
