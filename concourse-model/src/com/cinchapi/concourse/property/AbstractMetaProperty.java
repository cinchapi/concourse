package com.cinchapi.concourse.property;

import com.cinchapi.concourse.annotations.DataType;
import com.cinchapi.concourse.model.MetaProperty;

/**
 * The abstract base implementation of the {@link MetaProperty} interface.
 * @author jnelson
 *
 * @param <T>
 */
@DataType("abstract")
public abstract class AbstractMetaProperty<T> extends AbstractProperty<T> implements MetaProperty<T>{

	public AbstractMetaProperty(String key, T value) {
		super(key, value);
	}

}
