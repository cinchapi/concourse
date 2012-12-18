package com.cinchapi.concourse.property.meta;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.MetaProperty;
import com.cinchapi.concourse.property.AbstractMetaProperty;

/**
 * The <code>title</code> {@link MetaProperty}.
 * @author jnelson
 *
 */
@DataType("string")
public class Title extends AbstractMetaProperty<String> {
	
private static final String key = "title";
	
	/**
	 * Create a new {@link Title}.
	 * @param value
	 */
	public Title(String value){
		this(key, value);
	}
	
	private Title(String key, String value){
		super(key, value);
	}

}
