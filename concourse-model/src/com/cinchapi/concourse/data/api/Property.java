package com.cinchapi.concourse.data;

import com.cinchapi.commons.annotations.UnitTested;
import com.cinchapi.concourse.annotations.Immutable;

/**
 * An immutable attribute that is uniquely describable by the mapping of a <code>key</code> to a <code>value</code>.
 * @author jnelson
 *
 * @param <T> - the <code>value</code> type
 */
@Immutable
@UnitTested(PropertyTest.class)
public interface Property<T>{
	
	/**
	 * Return the encapsulated <code>key</code>.
	 * @return the <code>key</code>.
	 */
	public String getKey();
	
	/**
	 * Return the encapsulated <code>value</code>.
	 * @return the <code>value</code>.
	 */
	public T getValue();
	
	/**
	 * Return a string that describes the data type for this <code>property</code>. The type should be specified in each subclass using the 
	 * {@link DataType} annotation.
	 * @return the data <code>type</code>.
	 */
	public String getType();
	
	/**
	 * Two <code>property</code> objects are equal if they have the same <code>key</code>, <code>value</code> 
	 * and <code>type</code>.
	 */
	@Override
	public boolean equals(Object obj);
	
	
}
