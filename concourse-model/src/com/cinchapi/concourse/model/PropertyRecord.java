package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.UnitTested;
import com.cinchapi.concourse.data.Property;
import com.cinchapi.concourse.model.PropertyRecordTest;

/**
 * A record of the association between an {@link Entity} and a {@link Property} for an interval of time. 
 * When adding a <code>property</code> to an <code>entity</code>, create a new property <code>record</code> 
 * associating the two. If the <code>property</code> is removed, simply mark the analogous <code>record</code>
 * as removed.
 * 
 * @author jnelson
 *
 */
@UnitTested(PropertyRecordTest.class)
public interface PropertyRecord<T> {
	
	/**
	 * Mark this <code>record</code> as removed when the encapsulated <code>property</code> is removed from
	 * the <code>entity</code>.
	 * @return the <code>removed</code> timestamp. This operation is idempotent, so subsequent calls following
	 * the first will return the same timestamp.
	 */
	public DateTime markAsRemoved();
	
	/**
	 * Check to see if the <code>record</code> is marked as removed.
	 * @return <code>true</code> if the <code>record</code> is marked as removed.
	 */
	public boolean isMarkAsRemoved();
	
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
	 * Get the timestamp that indicates when the <code>property</code> was <code>added</code> to the <code>entity</code>.
	 * @return the <code>added </code> timestamp.
	 */
	public DateTime getAddedTime();
	
	/**
	 * Get the timestamp that indicates when the <code>property</code> was <code>removed</code> from the <code>entity</code>.
	 * @return the <code>removed </code> timestamp or <code>null</code> if <code>{@link #isMarkAsRemoved()}</code> is <code>false</code>.
	 */
	public DateTime getRemovedTime();
	
	/**
	 * Check to see if this <code>record</code> is equal to another <code>object</code>.
	 * <p>
	 * Two {@link PropertyRecord} objects, <code>A</code> and <code>B</code> are equal under the following scenarios:
	 * 	<ul>
	 * 		<li><pre>!A.{@link #isMarkAsRemoved()} && !A.{@link #isMarkAsRemoved()} && A.{@link #getEntity()}.equals(B.{@link #getEntity()});
	 * 		&& A.{@link #getProperty()}.equals(B.{@link #getProperty()});</pre></li>
	 * 		<li><pre>A.{@link #isMarkAsRemoved()} && A.{@link #isMarkAsRemoved()} && A.{@link #getEntity()}.equals(B.{@link #getEntity()})
	 * 		&& A.{@link #getProperty()}.equals(B.{@link #getProperty()}) && A.{@link #getAddedTime()}.equals(B.{@link #getAddedTime()})
	 * 		&& A.{@link #getRemovedTime()}.equals(B.{@link #getRemovedTime()}); </pre></li>
	 * 	</ul>
	 * </p>
	 * @param obj
	 * @return <code>true</code> if the two <code>this</code> and <code>obj</code> are equal.
	 */
	@Override
	public boolean equals(Object obj);
	
	

}
