package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;

/**
 * An {@link AbstractProperty} that holds variable length string.
 * @author jnelson
 *
 */
@DataType("string")
public class StringProperty extends AbstractProperty<String>{

	/**
	 * Create a new {@link StringProperty}
	 * @param key
	 * @param value
	 */
	public StringProperty(String key, String value) {
		super(key, value);
	}

}
