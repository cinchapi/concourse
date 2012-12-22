package com.cinchapi.concourse.model.api;

import java.util.List;

import org.joda.time.DateTime;

import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.property.api.Property;

/**
 * A {@link Modification} occurs when a {@link Property} is <code>added</code> to or <code>removed</code> from an {@link Entity}
 * at a <code>timestamp</code>. Each has a unique <code>lookup</code> key.
 * 
 * @author jnelson
 * 
 * @param <T> - the {@link Property} <code>type</code>
 *
 */
@Immutable
public interface Modification<T> {
	
	/**
	 * Enum that specifies the <code>type</code> for a {@link Modification}.
	 */
	enum Type{
		PROPERTY_ADDED,
		PROPERTY_REMOVED
	}
	
	/**
	 * Get the encapsulated <code>entity</code>.
	 * @return the <code>entity</code>.
	 */
	public Entity getEntity();
	
	/**
	 * Get the encapsulated <code>property</code>.
	 * @return the <code>property</code>.
	 */
	public Property<T> getProperty();
	
	/**
	 * Get the modification <code>timestamp</code>.
	 * @return the <code>timestamp</code>.
	 */
	public DateTime getTimestamp();
	
	/**
	 * Get the modification <code>type</code>.
	 * @return the <code>type</code>.
	 */
	public Type getType();
	
	/**
	 * Get the <code>lookup</code> key.
	 * @return the <code>lookup</code>.
	 */
	public String getLookup();
	
	/**
	 * Get the components of this {@link Modification} as a list.
	 * @return the list.
	 */
	public List<String> asList();
	
	

}
