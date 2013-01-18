package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

/**
 * An immutable collection of intrinsic attributes that are essential to any
 * {@link Thing}.
 * The relevant components are
 * <ul>
 * <li><strong>classifier</strong> - a classification string</li>
 * <li><strong>label</strong> - a non-unique name</li>
 * <li><strong>created</strong> - the timestamp when the {@link Thing} was
 * created</li>
 * <li><strong>deleted</strong> - the timestamp when the {@link Thing} was
 * deleted</li>
 * </ul>
 * 
 * @author jnelson
 */
interface Metadata {

	/**
	 * Return the <code>classifier</code>.
	 * 
	 * @return the <code>classifier</code>/
	 */
	public String getClassifier();

	/**
	 * Return the <code>label</code>.
	 * 
	 * @return the <code>label</code>.
	 */
	public String getLabel();

	/**
	 * Return the <code>created</code> timestamp.
	 * 
	 * @return the <code>created</code> timestamp.
	 */
	public DateTime getCreated();

	/**
	 * Return the <code>deleted</code> timestamp.
	 * 
	 * @return the <code>deleted</code> timestamp.
	 */
	public DateTime getDeleted();

}
