package com.cinchapi.concourse.metadata;

import com.cinchapi.concourse.data.Property;

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
