package com.cinchapi.concourse.model;

import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;

import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Metadata;
import com.cinchapi.concourse.property.IntrinsicClassProperty;
import com.cinchapi.concourse.property.IntrinsicCreatedProperty;
import com.cinchapi.concourse.property.IntrinsicTitleProperty;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * Abstract implementation of the {@link Metadata} interface.
 * 
 * @author jnelson
 *
 */
public abstract class AbstractMetadata implements Metadata{
	
	private final Entity entity;
	private final Map<String, IntrinsicProperty<?>> properties;
	
	/* Non-Initializable */
	public AbstractMetadata(Entity entity, String classifier, String title){
		this(entity, classifier, title, DateTime.now());
	}
	
	/* Non-Initializable */
	protected AbstractMetadata(Entity entity, String classifier, String title, DateTime created){
		this.entity = entity;
		this.properties = createEmtptyPropertiesMap();
		this.properties.put(CLASS_KEY, new IntrinsicClassProperty(classifier));
		this.properties.put(TITLE_KEY, new IntrinsicTitleProperty(title));
		this.properties.put(CREATED_KEY, new IntrinsicCreatedProperty(created));
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
	
	private void setTitle(String title){
		properties.put(TITLE_KEY, new IntrinsicTitleProperty(title));
	}
	
	/**
	 * Create an empty {@link #properties} map.
	 * @return an empty <code>properties</code> map.
	 */
	protected abstract Map<String, IntrinsicProperty<?>> createEmtptyPropertiesMap();

}
