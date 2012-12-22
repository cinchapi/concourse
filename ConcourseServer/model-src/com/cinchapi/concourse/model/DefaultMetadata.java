package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.Map;

import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Metadata;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * Default implementation of the {@link Metadata} interface.
 * @author jnelson
 *
 */
public class DefaultMetadata extends AbstractMetadata{

	/**
	 * Create new {@link DefaultMetadata} for the <code>entity</code> described by the <code>classifier</code> 
	 * and <code>title</code>.
	 * @param entity
	 * @param classifier
	 * @param title
	 */
	public DefaultMetadata(Entity entity, String classifier, String title) {
		super(entity, classifier, title);
	}

	@Override
	protected Map<String, IntrinsicProperty<?>> createEmtptyPropertiesMap() {
		return new HashMap<String, IntrinsicProperty<?>>();
	}


}
