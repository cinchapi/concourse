package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

import com.google.common.primitives.UnsignedLong;

/**
 * <p>
 * A record that describes a modification to a {@link Thing} concerning the
 * mapping of a <code>key</code> to a </code>value</code>.
 * </p>
 * <p>
 * <h2>Components</h2>
 * <ul>
 * <li><strong>primary_key</strong> - A unique identifier</li>
 * <li><strong>Locator</strong> - A grouping key calculated from the
 * <code>thing_id</code>, <code>key</code>, <code>value</code> and
 * <code>value_type</code>. An even number of revisions with the same locator
 * implies that {@link Thing#contains(String, Object)} for the encoded
 * <code>key</code> and <code>value</code> returns <code>false</code>. An odd
 * number implies that the function call will return <code>true</code></li>
 * <li><strong>thing_id</strong> - A referenced to the associated {@link Thing}</li>
 * <li><strong>key</strong> - The mapping key</li>
 * <li><strong>value</strong> - The mapped value</li>
 * <li><strong>value_type</strong> - A description of the value type</li>
 * <li><strong>timestamp</strong> - The associated timestamp</li>
 * </ul>
 * </p>
 * 
 * @author jnelson
 * @param <T> - the value <code>type</code>
 */
interface Revision<T> {

	/**
	 * Return the unique primary key.
	 * 
	 * @return the primary key.
	 */
	public UnsignedLong getPrimaryKey();

	/**
	 * Return the property <code>locator</code>, which is calculated using the
	 * <code>thing_id</code>, <code>key</code>, <code>value</code> and
	 * <code>value_type</code>.
	 * 
	 * @return the <code>locator</code>
	 */
	public String getLocator();

	/**
	 * Get the <code>id</code> of the associated <code>thing</code>.
	 * 
	 * @return the <code>id</code>.
	 */
	public UnsignedLong getThingId();

	/**
	 * Return the associated <code>key</code>.
	 * 
	 * @return the <code>key</code>
	 */
	public String getKey();

	/**
	 * Return the associated <code>value</code>.
	 * 
	 * @return the <code>value</code>
	 */
	public T getValue();

	/**
	 * Return a string description of the <code>value</code> type.
	 * 
	 * @return the <code>value</code> type.
	 */
	public String getValueType();

	/**
	 * Get the associated <code>timestamp</code>.
	 * 
	 * @return the <code>timestamp</code>.
	 */
	public DateTime getTimestamp();

	/**
	 * Two revisions are equal if they have the same <code>locator</code>.
	 * 
	 * @param obj
	 * @return <code>true</code> if <code>obj</code> is the same type and has
	 *         the same <code>locator</code>.
	 */
	@Override
	public boolean equals(Object obj);

}
