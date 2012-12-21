package com.cinchapi.concourse.property.api;


/**
 * An essential {@link Property}.
 * @author jnelson
 *
 * @param <T> - the <code>value</code> type
 */
public interface IntrinsicProperty<T> extends Property<T>{
	
	/**
	 * Get this instance.
	 * @return this instance.
	 */
	public Property<T> getThisIntrinsicProperty();

}
