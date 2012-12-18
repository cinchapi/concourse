package com.cinchapi.concourse.model.mock;

import com.cinchapi.commons.util.Counter;
import com.cinchapi.concourse.id.IdGenerator;

/**
 * A {@link MockId} generator.
 * @author jnelson
 *
 */
public class MockIdGenerator implements IdGenerator{
	
	Counter counter;
	
	public MockIdGenerator(){
		this(new Counter());
	}
	
	public MockIdGenerator(Counter counter){
		this.counter = counter;
	}

	@Override
	public MockLongId requestId() {
		return new MockLongId(counter.next());
	}

}
