package com.cinchapi.concourse.property;

import org.joda.time.DateTime;

import com.cinchapi.concourse.annotations.DataType;

/**
 * An {@link AbstractProperty} that specifies a point in time.
 * @author jnelson
 *
 */
@DataType("datetime")
public class DateTimeProperty extends AbstractProperty<DateTime>{

	/**
	 * Create a new {@link DateTimeProperty}.
	 * @param key
	 * @param value
	 */
	public DateTimeProperty(String key, DateTime value) {
		super(key, value);
	}

}
