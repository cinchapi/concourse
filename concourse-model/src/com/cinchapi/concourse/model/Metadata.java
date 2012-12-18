package com.cinchapi.concourse.model;

import java.util.Iterator;


/**
 * A Collection of {@link MetaProperty} objects that describe an {@link Entity}. 
 * 
 * @author jnelson
 *
 */
@SuppressWarnings("rawtypes")
public interface Metadata extends Iterable<MetaProperty>{
	
	/**
	 * Get the <code>property</code> described by the <code>key</code>.
	 * @param key
	 * @return the <code>property</code> or <code>null</code> if none is found.
	 */
	public MetaProperty get(String key);
	
	/**
	 * Check to see if this <code>record</code> is the metadata for the <code>entity</code>.
	 * @param entity
	 * @return <code>true</code> if this <code>record</code> represents the <code>entity</code>.
	 */
	public boolean isMetadataFor(Entity entity);
	
	/**
	 * Set the <code>value</code> of the <code>property</code> described by the <code>key</code>.
	 * @param key
	 * @param value
	 * @return the updated <code>property</code>.
	 * @throws UnsupportedOperationException if this method is unsupported.
	 */
	public <T> MetaProperty set(String key, T value) throws UnsupportedOperationException;
	
	/**
	 * Return an {@link Iterator} over the encapsulated <code>properties</code>.
	 * @return the <code>iterator</code>
	 */
	@Override
	public Iterator<MetaProperty> iterator();
	

}
