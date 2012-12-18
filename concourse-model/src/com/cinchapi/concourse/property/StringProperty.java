package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.Property;

/**
 * A {@link Property} that holds variable length string.
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
