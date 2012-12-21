package com.cinchapi.concourse.property;

import org.joda.time.DateTime;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * An {@link IntrinsicProperty} that specifies a created <code>value</code>.
 * @author jnelson
 *
 */
@DataType("datetime")
public class CreatedIntrinsicProperty extends AbstractProperty<DateTime> implements IntrinsicProperty<DateTime>{
	
	private static final String key = "created";
	
	/**
	 * Create a new {@link CreatedIntrinsicProperty} with the specified <code>value</code>.
	 * @param value
	 */
	public CreatedIntrinsicProperty(DateTime value){
		this(key, value);
	}
	
	@NoDocumentation
	private CreatedIntrinsicProperty(String key, DateTime value){
		super(key, value);
	}

	@Override
	public AbstractProperty<DateTime> getThisIntrinsicProperty() {
		return this;
	}

}

