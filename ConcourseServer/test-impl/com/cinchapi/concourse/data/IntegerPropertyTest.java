package com.cinchapi.concourse.data;

import java.util.Random;

import com.cinchapi.concourse.property.IntegerProperty;
import com.cinchapi.concourse.property.api.Property;

public class IntegerPropertyTest extends PropertyTest<Integer>{

	@Override
	public Property<Integer> getInstance(String key, Integer value) {
		return new IntegerProperty(key, value);
	}

	@Override
	public Integer getRandomValue() {
		Random random = new Random();
		return random.nextInt();
	}

}
