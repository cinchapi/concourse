package com.cinchapi.concourse.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;

import com.cinchapi.commons.util.Hash;
import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.Modification;
import com.cinchapi.concourse.property.api.Property;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Abstract implementation of the {@link Modification} interface.
 * @author jnelson
 *
 * @param <T> - the {@link Property} <code>type</code>
 */
@Immutable
public abstract class AbstractModification<T> implements Modification<T>{
	
	private Entity entity;
	private Property<T> property;
	private DateTime timestamp;
	private Type type;
	
	/* lazy initialization */
	private String lookup;
	private int hashCode;
	
	private static final Gson json = new Gson();
	
	/* Non-Initializable */
	public AbstractModification(Entity entity, Property<T> property, Type type){
		this(entity, property, DateTime.now(), type);
	}
	
	/* Non-Initializable */
	public AbstractModification(Entity entity, Property<T> property, DateTime timestamp, Type type){
		this.entity = entity;
		this.property = property;
		this.timestamp = timestamp;
		this.type = type;
	}

	@Override
	public Entity getEntity() {
		return entity;
	}

	@Override
	public Property<T> getProperty() {
		return property;
	}

	@Override
	public DateTime getTimestamp() {
		return timestamp;
	}
	
	/**
	 * For global uniquness, the <code>lookup</code> is derived from {@link #entity}, {@link #property}, {@link #timestamp}
	 * and {@link #type}.
	 */
	@Override 
	public String getLookup(){
		if(lookup == null){
			String rawLookup = new StringBuilder()
			.append(entity)
			.append(property)
			.append(timestamp)
			.append(type)
			.toString();
			lookup = Hash.sha1(rawLookup);
		}
		return lookup;
	}

	@Override
	public Modification.Type getType() {
		return type;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj == null){
			return false;
		}
		else if(obj == this){
			return true;
		}
		else if(obj.getClass() != this.getClass()){
			return false;
		}
		else{
			@SuppressWarnings("unchecked")
			AbstractModification<T> other = (AbstractModification<T>) obj;
			return new EqualsBuilder().append(getLookup(), other.getLookup()).isEquals();
		}
	}
	
	@Override
	public int hashCode(){
		if(hashCode == 0){
			HashCodeBuilder builder = new HashCodeBuilder();
			builder.append(entity);
			builder.append(property);
			builder.append(timestamp);
			builder.append(type);
			hashCode = builder.toHashCode();
		}
		return hashCode;
	}
	
	@Override
	public String toString(){
		JsonObject object = new JsonObject();
		object.addProperty("id", entity.getId().toString());
		object.addProperty("key", property.getKey());
		object.addProperty("value", property.getValue().toString());
		object.addProperty("type", type.toString());
		object.addProperty("timestamp", timestamp.getMillis());
		return json.toJson(object);
	}
	

}
