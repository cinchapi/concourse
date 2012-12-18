package com.cinchapi.concourse.model.simple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.joda.time.DateTime;


import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.commons.annotations.NonDefensiveCopyTrackChanges;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.MetaProperty;
import com.cinchapi.concourse.model.Metadata;
import com.cinchapi.concourse.property.meta.Created;
import com.cinchapi.concourse.property.meta.Title;

/**
 * The simple implementation of the {@link Metadata} interface.
 * The relevant <code>properties</code> are: 
 * <ul>
 * 	<li><strong>class</strong> -  A string that describes that nature of the <code>entity</code></li>
 * 	<li><strong>title</strong> -  A string that names the <code>entity</code></li>
 * 	<li><strong>created</strong> -  The timestamp when the <code>entity</code> was created</li>
 * </ul>
 * @author jnelson
 *
 */
@SuppressWarnings("rawtypes")
public class SimpleMetadata implements Metadata{
	
	public static final String CLASS_KEY = "class";
	public static final String TITLE_KEY = "title";
	public static final String CREATED_KEY = "created";

	protected Entity entity;
	private Map<String, MetaProperty> properties = new HashMap<String, MetaProperty>();
	
	/**
	 * Create a new {@link SimpleMetadata} for the <code>entity</code> with the <code>classifier</code>
	 * and <code>title</code>, </code>created</code> now.
	 * @param id
	 * @param classifier
	 * @param title
	 */
	public SimpleMetadata(Entity entity, String classifier, String title){
		this(entity, classifier, title, DateTime.now());	
	}
	
	/**
	 * Create a new {@link SimpleMetadata} for the <code>entity</code> with the <code>classifier</code>
	 * and <code>title</code>, <code>created</code> at the specified timestamp.
	 * @param entity
	 * @param classifier
	 * @param title
	 * @param created
	 */
	public SimpleMetadata(Entity entity, String classifier, String title, DateTime created){
		setEntity(entity);
		setClass(classifier);
		setTitle(title);
		setCreated(created);
	}
	
	@Override
	public MetaProperty get(String key) {
		return properties.get(key);
	}
	
	@Override
	public boolean isMetadataFor(Entity entity){
		return this.entity.equals(entity);
	}

	@Override
	public<T> MetaProperty set(String key, T value) throws UnsupportedOperationException {
		if(key.equalsIgnoreCase(TITLE_KEY)){
			setTitle((String) value);
			return properties.get(TITLE_KEY);
		}
		else{
			throw new UnsupportedOperationException("Cannot set the value of the "+key+" property.");
		}
	}
	
	@Override
	public Iterator<MetaProperty> iterator() {
		return properties.values().iterator();
	}
	
	@NonDefensiveCopyTrackChanges
	protected void setEntity(Entity entity){
		this.entity = entity;
	}
	
	@NoDocumentation
	protected void setClass(String classifier){
		properties.put(CLASS_KEY, new com.cinchapi.concourse.property.meta.Class(classifier));
	}
	
	@NoDocumentation
	protected void setTitle(String title){
		properties.put(TITLE_KEY, new Title(title));
	}
	
	@NoDocumentation
	protected void setCreated(DateTime created){
		properties.put(CREATED_KEY, new Created(created));
	}
}
