package com.cinchapi.concourse.id;

/**
 * An {@link Id} generator.
 * @author jnelson
 *
 */
public interface IdGenerator {
	
	/**
	 * Request an {@link Id}.
	 * @return the <code>id</code>.
	 */
	public Id requestId();

}
