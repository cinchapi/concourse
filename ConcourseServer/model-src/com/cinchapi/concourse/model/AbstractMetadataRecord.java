package com.cinchapi.concourse.model;

import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.MetadataRecord;
import com.cinchapi.concourse.property.ClassIntrinsicProperty;
import com.cinchapi.concourse.property.CreatedIntrinsicProperty;
import com.cinchapi.concourse.property.TitleIntrinsicProperty;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

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
		this.properties = createEmtptyPropertiesMap();
		this.properties.put(CLASS_KEY, new ClassIntrinsicProperty(classifier));
		this.properties.put(TITLE_KEY, new TitleIntrinsicProperty(title));
		this.properties.put(CREATED_KEY, new CreatedIntrinsicProperty(created));
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
	 * Create an empty {@link #properties} map.
	 * @return an empty <code>properties</code> map.
	 */
	protected abstract Map<String, IntrinsicProperty<?>> createEmtptyPropertiesMap();

}
