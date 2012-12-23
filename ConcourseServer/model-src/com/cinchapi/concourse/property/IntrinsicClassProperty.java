package com.cinchapi.concourse.property;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * An {@link IntrinsicProperty} that specifies a class <code>value</code>.
 * @author jnelson
 *
 */
@DataType("string")
public class IntrinsicClassProperty extends AbstractProperty<String> implements IntrinsicProperty<String>{
	
	private static final String key = "class";
	
	/**
	 * Create a new {@link IntrinsicClassProperty} with the specified <code>value</code>.
	 * @param value
	 */
	public IntrinsicClassProperty(String value){
		this(key, value);
	}
	
	@NoDocumentation
	private IntrinsicClassProperty(String key, String value){
		super(key, value);
	}

	@Override
	public AbstractProperty<String> getThisIntrinsicProperty() {
		return this;
	}

}
