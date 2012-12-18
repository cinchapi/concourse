package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.Property;

/**
 * A {@link Property} that holds a 32-bit numeric value.
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
