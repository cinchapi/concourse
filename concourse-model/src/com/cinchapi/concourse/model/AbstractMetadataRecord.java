package com.cinchapi.concourse.model;

import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.metadata.IntrinsicProperty;
import com.cinchapi.concourse.metadata.TitleIntrinsicProperty;

/**
 * Abstract implementation of the {@link MetadataRecord} interface.
 * 
 * @author jnelson
 *
 */
public abstract class AbstractMetadataRecord implements MetadataRecord{
	
	protected Entity entity;
	protected Map<String, IntrinsicProperty<?>> properties;
	
	@NoDocumentation
	public AbstractMetadataRecord(Entity entity, String classifier, String title){
		this(entity, classifier, title, DateTime.now());
	}
	
	@NoDocumentation
	protected AbstractMetadataRecord(Entity entity, String classifier, String title, DateTime created){
		this.entity = entity;
		this.properties = createPropertiesMap(classifier, title, created);
	}
	
	@Override
	public IntrinsicProperty<?> get(String key) {
		return properties.get(key);
	}
	
	@Override
	public boolean isMetadataFor(Entity entity){
		return this.entity.equals(entity);
	}

	@Override
	public IntrinsicProperty<?> set(String key, Object value) throws UnsupportedOperationException {
		if(key.equalsIgnoreCase(TITLE_KEY)){
			setTitle((String) value);
			return properties.get(TITLE_KEY);
		}
		else{
			throw new UnsupportedOperationException("Cannot set the value of the "+key+" property.");
		}
	}
	
	@Override
	public Iterator<IntrinsicProperty<?>> iterator() {
		return properties.values().iterator();
	}
	
	@NoDocumentation
	protected void setTitle(String title){
		properties.put(TITLE_KEY, new TitleIntrinsicProperty(title));
	}
	
	/**
	 * Create the {@link #properties} map for newly created <code>metadata</code>.
	 * @param classifier
	 * @param title
	 * @param created
	 * @return the <code>properties</code> map.
	 */
	protected abstract Map<String, IntrinsicProperty<?>> createPropertiesMap(String classifier, String title, DateTime created);

}
