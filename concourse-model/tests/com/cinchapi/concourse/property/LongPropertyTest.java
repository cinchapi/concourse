package com.cinchapi.concourse.property;

import java.util.Random;

import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.PropertyTest;
import com.cinchapi.concourse.property.LongProperty;

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
