package com.cinchapi.concourse.store;

import com.cinchapi.commons.util.Hash;
import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.property.api.Property;

/**
 * A {@link Mutation} that uses a SHA-1 hash locator.
 * @author jnelson
 *
 */
@Immutable
public class Sha1LocatorMutation extends AbstractMutation{

	/**
	 * Create a {@link Sha1LocatorMutation}.
	 * @param id
	 * @param property
	 * @param mutationType
	 */
	public Sha1LocatorMutation(Id id, Property<?> property, Type mutationType) {
		super(id, property, mutationType);
	}

	@Override
	public String getLocator() {
		return Hash.sha1(id+key+value+valueType+mutationType);
	}

}
