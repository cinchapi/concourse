package com.cinchapi.concourse.property;

import java.util.Random;

import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.PropertyTest;
import com.cinchapi.concourse.property.IntegerProperty;

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
