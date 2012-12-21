package com.cinchapi.concourse.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.model.api.Entity;
import com.cinchapi.concourse.model.api.PropertyRecord;
import com.cinchapi.concourse.property.api.Property;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Abstract implementation of the {@link PropertyRecord} interface/
 * @author jnelson
 *
 * @param <T>
 */
public class AbstractPropertyRecord<T> implements PropertyRecord<T>{
	
	protected Entity entity;
	protected Property<T> property;
	protected DateTime added;
	protected DateTime removed;
	
	private static final Gson json = new Gson();
	
	@NoDocumentation
	public AbstractPropertyRecord(Entity entity, Property<T> property){
		this(entity, property, DateTime.now());
	}
	
	@NoDocumentation
	public AbstractPropertyRecord(Entity entity, Property<T> property, DateTime added){
		this(entity, property, added, null);
	}
	
	@NoDocumentation
	private AbstractPropertyRecord(Entity entity, Property<T> property, DateTime added, DateTime removed){
		this.entity = entity;
		this.property = property;
		this.added = added;
		this.removed = removed;
	}
	
	@Override
	public DateTime markAsRemoved(){
		removed = !isMarkAsRemoved() ? DateTime.now() : removed;
		return removed;
	}
	
	@Override
	public boolean isMarkAsRemoved(){
		return removed != null;
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
	public DateTime getAddedTime() {
		return added;
	}
	
	@Override
	public DateTime getRemovedTime() {
		return removed;
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
			AbstractPropertyRecord<T> other = (AbstractPropertyRecord<T>) obj;
			EqualsBuilder builder = new EqualsBuilder();
			if(removed == null && other.removed == null){ //both exist, so compare entity and property only
				return builder.append(entity, other.entity)
						.append(property, other.property)
						.isEquals();
			}
			else if(removed != null && other != null){ //neither exists, so compare all fields
				return builder.append(entity, other.entity)
						.append(property, other.property)
						.append(added, other.added)
						.append(removed, other.removed)
						.isEquals();
			}
			else{ //one exists and the other does not, so can't be equal because hashCode() will differ
				return false;
			}
		}
	}
	
	@Override
	public int hashCode(){
		HashCodeBuilder builder = new HashCodeBuilder();
		builder.append(entity);
		builder.append(property);
		if(removed != null){ //does not exist, so compute using all fields
			builder.append(added);
			builder.append(removed);
		}
		return builder.toHashCode();
	}
	
	@Override
	public String toString(){
		JsonObject object = new JsonObject();
		object.addProperty("id", entity.getId().toString());
		object.addProperty("key", property.getKey());
		object.addProperty("value", property.getValue().toString());
		object.addProperty("added", added.getMillis());
		if(removed != null){
			object.addProperty("removed", removed.getMillis());
		}
		return json.toJson(object);
	}

}
