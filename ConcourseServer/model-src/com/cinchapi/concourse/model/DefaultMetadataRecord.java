package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.Map;

import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.MetadataRecord;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * Default implementation of the {@link MetadataRecord} interface.
 * @author jnelson
 *
 */
public class DefaultMetadataRecord extends AbstractMetadataRecord{

	/**
	 * Create new {@link DefaultMetadataRecord} for the <code>entity</code> described by the <code>classifier</code> 
	 * and <code>title</code>.
	 * @param entity
	 * @param classifier
	 * @param title
	 */
	public DefaultMetadataRecord(Entity entity, String classifier, String title) {
		super(entity, classifier, title);
	}

	@Override
	protected Map<String, IntrinsicProperty<?>> createEmtptyPropertiesMap() {
		return new HashMap<String, IntrinsicProperty<?>>();
	}


}
