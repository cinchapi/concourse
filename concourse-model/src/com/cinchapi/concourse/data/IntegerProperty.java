package com.cinchapi.concourse.data;

import com.cinchapi.concourse.annotations.DataType;

/**
 * A {@link AbstractProperty} that holds a 32-bit numeric value.
 * @author jnelson
 *
 */
@DataType("int")
public class IntegerProperty extends AbstractProperty<Integer>{

	/**
	 * Create a new {@link IntegerProperty}.
	 * @param key
	 * @param value
	 */
	public IntegerProperty(String key, Integer value) {
		super(key, value);
	}

}
