package com.cinchapi.concourse.store.api;

import java.util.List;

import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.property.api.Property;

/**
 * A record of a {@link Property} addition to or removal from an {@link Entity}.
 * @author jnelson
 *
 */
public interface Mutation {
	
	/**
	 * The {@link Mutation} type.
	 * @author jnelson
	 *
	 */
	public enum Type{
		ADDITION,
		REMOVAL
	}
	
	/**
	 * Return a {@link List} of the components in the following order:
	 * <ul>
	 * 	<li>locator</li>
	 * 	<li>id</li>
	 * 	<li>key</li>
	 * 	<li>value</li>
	 * 	<li>valueType</li>
	 * 	<li>mutationType</li>
	 * 	<li>timestamp</li>
	 * </ul>
	 * @return
	 */
	public List<String> asList();
	
	/**
	 * Get the <code>locator</code> for this {@link Mutation}.
	 * @return
	 */
	public String getLocator();
	
	/**
	 * Get the <code>id</code> of the {@link Entity}.
	 * @return the <code>id</code>
	 */
	public String getId();
	
	/**
	 * Get the <code>key</code> of the {@link Property}.
	 * @return the <code>key</code>
	 */
	public String getKey();
	
	/**
	 * Get the <code>value</code> of the {@link Property}.
	 * @return the <code>value</code>
	 */
	public String getValue();
	
	/**
	 * Get the value <code>type</code> of the {@link Property}.
	 * @return the value <code>type</code>
	 */
	public String getValueType();
	
	/**
	 * Get the <code>mutation type</code>.
	 * @return the <code>mutation type</code>
	 */
	public String getMutationType();
	
	/**
	 * Get the <code>timestamp</code> associated with this mutation.
	 * @return the <code>timestamp</code>
	 */
	public String getTimestamp();
	
}
