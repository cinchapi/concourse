package com.cinchapi.concourse.model.simple;

import java.lang.reflect.Field;

import org.joda.time.DateTime;

import com.cinchapi.commons.util.RandomString;
import com.cinchapi.concourse.model.Entity;
import com.cinchapi.concourse.model.Property;
import com.cinchapi.concourse.model.PropertyRecord;
import com.cinchapi.concourse.model.PropertyRecordTest;
import com.cinchapi.concourse.model.mock.MockEntity;
import com.cinchapi.concourse.model.mock.MockProperty;
import com.cinchapi.concourse.model.simple.SimplePropertyRecord;

public class SimplePropertyRecordTest extends PropertyRecordTest{
	
	private static final RandomString random = new RandomString();

	@Override
	public PropertyRecord copy(PropertyRecord record) {
		PropertyRecord copy = new SimplePropertyRecord(record.getEntity(), record.getProperty(), record.getAddedTime());
		try {
			Field f = record.getClass().getDeclaredField("removed");
			f.setAccessible(true);
			f.set(copy, record.getRemovedTime());
			
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return copy;
	}

	@Override
	public <T> PropertyRecord getInstance(Entity entity, Property<T> property, DateTime added) {
		return new SimplePropertyRecord(entity, property, added);
	}

	@Override
	public <T> PropertyRecord getInstanceRemoved(Entity entity, Property<T> property, DateTime added) {
		SimplePropertyRecord record = new SimplePropertyRecord(entity, property, added);
		record.markAsRemoved();
		return record;
	}

	@Override
	public Entity getEntityInstance() {
		String classifier = random.nextString();
		String title = random.nextString();
		return new MockEntity(classifier, title);
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Property getPropertyInstance() {
		String key = random.nextString();
		String value = random.nextString();
		return getPropertyInstance(key, value);
	}
	
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Property getPropertyInstance(String key, String value) {
		return new MockProperty(key, value);
	}

}
