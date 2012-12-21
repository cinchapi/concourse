package com.cinchapi.concourse.model;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.property.AbstractProperty;
import com.cinchapi.concourse.property.api.Property;

/**
 * A {@link Property} used for unit tests.
 * @author jnelson
 *
 */
@DataType("mock")
public class MockProperty extends AbstractProperty<String>{

	public MockProperty(String key, String value) {
		super(key, value);
	}

}
