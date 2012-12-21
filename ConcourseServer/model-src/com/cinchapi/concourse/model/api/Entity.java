package com.cinchapi.concourse.model.api;

import java.util.Iterator;
import java.util.Set;

import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.property.api.Property;

/**
 * An element that contains {@link MetadataRecord} and a {@link PropertyRecord} set. 
 * @author jnelson
 *
 */
public interface Entity extends Iterable<String>{
	
	/**
	 * Add the <code>property</code> to this <code>entity</code> if it does not already exist. If 
	 * the <code>property</code> was previously added and subsequently removed, then it can be added
	 * again, resulting in a new {@link PropertyRecord}.  
	 * @param property
	 * @return the created property <code>record</code> or <code>null</code> if the <code>property</code> 
	 * cannot be added.
	 */
	public PropertyRecord<?> add(Property<?> property);
	
	/**
	 * Check to see if this <code>entity</code> contains the <code>property</code>.
	 * @param record
	 * @return <code>true</code> if the <code>property</code> is contained.
	 */
	public boolean contains(Property<?> property);
	
	/**
	 * Get the {@link Property} objects described by the specified <code>key</code> that currently 
	 * exist on this {@link Entity}.
	 * @param key
	 * @return a set of relevant {@link Property} objects
	 */
	public Set<Property<?>> get(String key);
	
	/**
	 * Get the <code>id</code>.
	 * @return the <code>id</code>
	 */
	public Id getId();
	
	/**
	 * Remove a {@link Property} from this {@link Entity}
	 * @param record
	 * @return the affected {@link IPropertyRecord} or <code>null</code> if the {@link Property} cannot be removed (i.e
	 * it does not exist on this {@link Entity}).
	 */
	public PropertyRecord<?> remove(Property<?> property);
	
	/**
	 * Get the <code>metadata</code>.
	 * @return the <code>metadata</code>.
	 */
	public MetadataRecord getMetadata();
	
	/**
	 * Set the <code>title</code> for this <code>entity</code>.
	 * @param title
	 * @return <code>true</code> if the title is changed.
	 */
	public boolean setTitle(String title);
	
	/**
	 * Return an {@link Iterator} over the <code>property</code> keys.
	 * @return the <code>iterator</code>.
	 */
	public Iterator<String> iterator();

}
