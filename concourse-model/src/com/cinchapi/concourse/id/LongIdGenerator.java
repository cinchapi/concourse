package com.cinchapi.concourse.id;

/**
 * A {@link LongId} generator.
 * @author jnelson
 *
 */
public class LongIdGenerator implements IdGenerator{

	@Override
	public LongId requestId() {
		return new LongId(1L);
	}

}
