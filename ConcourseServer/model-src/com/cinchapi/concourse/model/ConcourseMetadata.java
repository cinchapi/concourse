package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.Map;

import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Metadata;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * {@link Metadata} for Concourse.
 * @author jnelson
 *
 */
@Immutable
public final class ConcourseMetadata extends AbstractMetadata{

	/**
	 * Create new {@link ConcourseMetadata} for the <code>entity</code> described by the <code>classifier</code> 
	 * and <code>title</code>.
	 * @param entity
	 * @param classifier
	 * @param title
	 */
	public ConcourseMetadata(Entity entity, String classifier, String title) {
		super(entity, classifier, title);
	}

	@Override
	protected Map<String, IntrinsicProperty<?>> createEmtptyPropertiesMap() {
		return new HashMap<String, IntrinsicProperty<?>>();
	}


}
