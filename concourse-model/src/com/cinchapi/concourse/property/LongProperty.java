package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.Property;

/**
 * A {@link Property} that holds a 64-bit numberic value.
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
