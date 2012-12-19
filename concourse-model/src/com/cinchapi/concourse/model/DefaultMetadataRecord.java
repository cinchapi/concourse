package com.cinchapi.concourse.model;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.cinchapi.concourse.metadata.ClassIntrinsicProperty;
import com.cinchapi.concourse.metadata.CreatedIntrinsicProperty;
import com.cinchapi.concourse.metadata.IntrinsicProperty;
import com.cinchapi.concourse.metadata.TitleIntrinsicProperty;

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
	protected Map<String, IntrinsicProperty<?>> createPropertiesMap(String classifier, String title, DateTime created) {
		HashMap<String, IntrinsicProperty<?>> map = new HashMap<String, IntrinsicProperty<?>>();
		map.put(CLASS_KEY, new ClassIntrinsicProperty(classifier));
		map.put(TITLE_KEY, new TitleIntrinsicProperty(title));
		map.put(CREATED_KEY, new CreatedIntrinsicProperty(created));
		return map;
	}

}
