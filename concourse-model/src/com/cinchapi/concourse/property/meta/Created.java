package com.cinchapi.concourse.property.meta;

import org.joda.time.DateTime;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.MetaProperty;
import com.cinchapi.concourse.property.AbstractMetaProperty;

/**
 * The <code>created</code> {@link MetaProperty}.
 * @author jnelson
 *
 */
@DataType("datetime")
public class Created extends AbstractMetaProperty<DateTime> {
	
private static final String key = "created";
	
	/**
	 * Create a new {@link Created}.
	 * @param value
	 */
	public Created(DateTime value){
		this(key, value);
	}
	
	private Created(String key, DateTime value){
		super(key, value);
	}

}

