package com.cinchapi.concourse.model.util;

import com.cinchapi.concourse.model.SomeThing;
import com.cinchapi.concourse.model.Thing;
import com.google.common.primitives.UnsignedLong;

/**
 * A collection of utility methods for {@link Thing} objects.
 * 
 * @author jnelson
 */
public class Things {

	/**
	 * Create a new {@link SomeThing} with the specified <code>classifier</code>
	 * and <code>title</code>.
	 * 
	 * @param classifier
	 * @param label
	 * @return the new <code>thing</code>.
	 */
	public static SomeThing create(String classifier, String label){
		return new SomeThing(classifier, label);
	}

	/**
	 * Load an existing {@link SomeThing} identified by the specified
	 * <code>id</code>.
	 * 
	 * @param id
	 * @return the <code>thing</code>.
	 */
	public static SomeThing load(UnsignedLong id){
		return null;
	}

}
