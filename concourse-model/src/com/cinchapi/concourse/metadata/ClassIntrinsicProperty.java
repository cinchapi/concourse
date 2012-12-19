package com.cinchapi.concourse.metadata;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.data.AbstractProperty;

/**
 * An {@link IntrinsicProperty} that specifies a class <code>value</code>.
 * @author jnelson
 *
 */
@DataType("string")
public class ClassIntrinsicProperty extends AbstractProperty<String> implements IntrinsicProperty<String>{
	
	private static final String key = "class";
	
	/**
	 * Create a new {@link ClassIntrinsicProperty} with the specified <code>value</code>.
	 * @param value
	 */
	public ClassIntrinsicProperty(String value){
		this(key, value);
	}
	
	@NoDocumentation
	private ClassIntrinsicProperty(String key, String value){
		super(key, value);
	}

	@Override
	public AbstractProperty<String> getThisIntrinsicProperty() {
		return this;
	}

}
