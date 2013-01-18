package com.cinchapi.concourse.model.id;

import com.google.common.primitives.UnsignedLong;

public interface IDService {

	public UnsignedLong requestRandom();
	
	public UnsignedLong requestSequential();
}
