package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;

/**
 * An {@link AbstractProperty} that holds a 64-bit numberic value.
 * @author jnelson
 *
 */
@DataType("long")
public class LongProperty extends AbstractProperty<Long>{

	/**
	 * Create a new {@link LongProperty}.
	 * @param key
	 * @param value
	 */
	public LongProperty(String key, Long value) {
		super(key, value);
	}

}
