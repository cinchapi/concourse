package com.cinchapi.concourse.model;

import java.lang.reflect.Field;

import org.joda.time.DateTime;

import com.cinchapi.commons.util.RandomString;
import com.cinchapi.concourse.data.Property;
import com.cinchapi.concourse.model.mock.MockEntity;
import com.cinchapi.concourse.model.mock.MockProperty;

public class DefaultPropertyRecordTest<T> extends PropertyRecordTest<T>{
	
	private static final RandomString random = new RandomString();

	@Override
	public PropertyRecord<T> copy(PropertyRecord<T> record) {
		PropertyRecord<T> copy = new DefaultPropertyRecord<T>(record.getEntity(), record.getProperty(), record.getAddedTime());
		try {
			Field f = record.getClass().getSuperclass().getDeclaredField("removed");
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
	public PropertyRecord<T> getInstance(Entity entity, Property<T> property, DateTime added) {
		return new DefaultPropertyRecord<T>(entity, property, added);
	}

	@Override
	public PropertyRecord<T> getInstanceRemoved(Entity entity, Property<T> property, DateTime added) {
		DefaultPropertyRecord<T> record = new DefaultPropertyRecord<T>(entity, property, added);
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
