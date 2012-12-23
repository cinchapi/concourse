package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.id.Id;

/**
 * A {@link Property} that defines a relation to another {@link Entity}
 * @author jnelson
 *
 */
@DataType("relation")
public class RelationProperty extends AbstractProperty<Id>{

	/**
	 * Create a new {@link RelationProperty}.
	 * @param key
	 * @param value
	 */
	public RelationProperty(String key, Id value) {
		super(key, value);
	}

}
