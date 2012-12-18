package com.cinchapi.concourse.property.meta;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.MetaProperty;
import com.cinchapi.concourse.property.AbstractMetaProperty;

/**
 * The <code>class</code> {@link MetaProperty}.
 * @author jnelson
 *
 */
@DataType("string")
public class Class extends AbstractMetaProperty<String>{
	
	private static final String key = "class";
	
	/**
	 * Create a new {@link Class}.
	 * @param value
	 */
	public Class(String value){
		this(key, value);
	}
	
	private Class(String key, String value){
		super(key, value);
	}

}
