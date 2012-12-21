package com.cinchapi.concourse.model;

import java.util.Iterator;

import com.cinchapi.concourse.metadata.IntrinsicProperty;

/**
 * A Collection of {@link IntrinsicProperty} objects that describe an {@link Entity}.
 * The relevant <code>properties</code> are: 
 * <ul>
 * 	<li><strong>class</strong> -  A string that describes that nature of the <code>entity</code></li>
 * 	<li><strong>title</strong> -  A string that names the <code>entity</code></li>
 * 	<li><strong>created</strong> -  The timestamp when the <code>entity</code> was created</li>
 * </ul> 
 * 
 * @author jnelson
 *
 */
public interface MetadataRecord extends Iterable<IntrinsicProperty<?>>{
	
	public static final String CLASS_KEY = "class";
	public static final String TITLE_KEY = "title";
	public static final String CREATED_KEY = "created";
	
	/**
	 * Get the <code>property</code> described by the <code>key</code>.
	 * @param key
	 * @return the <code>property</code> or <code>null</code> if none is found.
	 */
	public IntrinsicProperty<?> get(String key);
	
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
	public IntrinsicProperty<?> set(String key, Object value) throws UnsupportedOperationException;
	
	/**
	 * Return an {@link Iterator} over the encapsulated <code>properties</code>.
	 * @return the <code>iterator</code>
	 */
	@Override
	public Iterator<IntrinsicProperty<?>> iterator();
	

}
