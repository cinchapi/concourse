package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

import com.cinchapi.commons.util.Hash;
import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.model.id.SimpleIDService;
import com.cinchapi.concourse.model.id.IDService;
import com.google.common.primitives.UnsignedLong;

/**
 * An implementation of the {@link Revision} interface for {@link SomeThing} objects.
 * @author jnelson
 *
 * @param <T> - the value type
 */
@Immutable
class SomeRevision<T> implements Revision<T>{
	
	private final UnsignedLong primaryKey;
	private final String locator;
	private final UnsignedLong thingId;
	private final String key;
	private final T value;
	private final String valueType;
	private final DateTime timestamp;
	
	private static final IDService ids = new SimpleIDService();
	
	/**
	 * Construct a revision at the current timestamp.
	 * @param thing
	 * @param key
	 * @param value
	 */
	public SomeRevision(Thing thing, String key, T value){
		this(thing, key, value, DateTime.now());
	}
	
	/**
	 * Construct a revision at the specified timestamp.
	 * @param thing
	 * @param key
	 * @param value
	 * @param timestamp
	 */
	public SomeRevision(Thing thing, String key, T value, DateTime timestamp){
		this.primaryKey = ids.requestSequential();
		this.thingId = thing.getId();
		this.key = key; //TODO what about reserved keywords?
		this.value = value;
		this.valueType = Locators.getValueType(value);
		this.locator = Locators.generate(this.thingId, this.key, this.value);
		this.timestamp = timestamp;
	}

	@Override
	public UnsignedLong getPrimaryKey(){
		return primaryKey;
	}

	@Override
	public String getLocator(){
		return locator;
	}

	@Override
	public UnsignedLong getThingId(){
		return thingId;
	}

	@Override
	public String getKey(){
		return key;
	}

	@Override
	public T getValue(){
		return value;
	}

	@Override
	public String getValueType(){
		return valueType;
	}

	@Override
	public DateTime getTimestamp(){
		return timestamp;
	}
	
	@Override
	public int hashCode(){
		final int prime = 31;
		int result = 1;
		result = prime * result + ((locator == null) ? 0 : locator.hashCode());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj){
		if(this == obj) return true;
		if(obj == null) return false;
		if(getClass() != obj.getClass()) return false;
		SomeRevision<T> other = (SomeRevision<T>) obj;
		if(locator == null){
			if(other.locator != null) return false;
		}
		else if(!locator.equals(other.locator)) return false;
		return true;
	}
	
	
	/**
	 * Utility functions for locators.
	 */
	static class Locators{
		
		/**
		 * Return a string description of the data type for <code>value</code>.
		 * @param value
		 * @return the <code>value</code> type.
		 */
		static<T> String getValueType(T value){
			return value.getClass().getSimpleName();
		}
		
		/**
		 * Calculate the <code>locator</code> value based on the specified parameters.
		 * @param thingId
		 * @param key
		 * @param value
		 * @return the <code>locator</code>.
		 */
		static<T> String generate(UnsignedLong thingId, String key, T value){
			String valueType = getValueType(value);
			return Hash.sha1(thingId+key+value+valueType);
		}
	}

}
