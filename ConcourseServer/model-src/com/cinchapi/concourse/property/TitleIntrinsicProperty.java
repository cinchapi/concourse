package com.cinchapi.concourse.property;

import com.cinchapi.commons.annotations.NoDocumentation;
import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.property.api.IntrinsicProperty;

/**
 * An {@link IntrinsicProperty} that specifies a title <code>value</code>.
 * @author jnelson
 *
 */
@DataType("string")
public class TitleIntrinsicProperty extends AbstractProperty<String> implements IntrinsicProperty<String>{
	
	private static final String key = "title";
	
	/**
	 * Create a new {@link TitleIntrinsicProperty} with the specified <code>value</code>.
	 * @param value
	 */
	public TitleIntrinsicProperty(String value){
		this(key, value);
	}
	
	@NoDocumentation
	private TitleIntrinsicProperty(String key, String value){
		super(key, value);
	}

	@Override
	public AbstractProperty<String> getThisIntrinsicProperty() {
		return this;
	}

}
