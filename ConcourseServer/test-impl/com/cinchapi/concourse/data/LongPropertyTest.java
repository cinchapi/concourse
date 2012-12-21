package com.cinchapi.concourse.data;

import java.util.Random;

import com.cinchapi.concourse.property.LongProperty;
import com.cinchapi.concourse.property.api.Property;

public class LongPropertyTest extends PropertyTest<Long>{

	@Override
	public Property<Long> getInstance(String key, Long value) {
		return new LongProperty(key, value);
	}

	@Override
	public Long getRandomValue() {
		Random random = new Random();
		return random.nextLong();
	}

}
