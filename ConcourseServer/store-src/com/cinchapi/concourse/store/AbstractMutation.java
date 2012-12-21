package com.cinchapi.concourse.store;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.cinchapi.concourse.annotations.Immutable;
import com.cinchapi.concourse.id.Id;
import com.cinchapi.concourse.property.api.Property;
import com.cinchapi.concourse.store.api.Mutation;

/**
 * Abstract implementation of the {@link Mutation} interface.
 * @author jnelson
 *
 */
@Immutable
public abstract class AbstractMutation implements Mutation{
	
	protected String id;
	protected String key;
	protected String value;
	protected String valueType;
	protected String mutationType;
	protected String timestamp;
	
	public AbstractMutation(Id id, Property<?> property, Type mutationType){
		this.id = id.toString();
		this.key = property.getKey();
		this.value = property.getValue().toString();
		this.valueType = property.getType();
		if(mutationType == Type.ADDITION){
			this.mutationType = "addition";
		}
		else{
			this.mutationType = "removal";
		}
		this.timestamp = Long.toString(DateTime.now().getMillis()); //TODO they should manually add the timestamp
	}

	@Override
	public List<String> asList() {
		ArrayList<String> list = new ArrayList<String>();
		list.add(getLocator());
		list.add(getId());
		list.add(getKey());
		list.add(getValue());
		list.add(getValueType());
		list.add(getMutationType());
		list.add(getTimestamp());
		return list;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public String getValueType() {
		return valueType;
	}

	@Override
	public String getMutationType() {
		return mutationType;
	}

	@Override
	public String getTimestamp() {
		return timestamp;
	}

}
