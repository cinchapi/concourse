package com.cinchapi.concourse.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation specifying the data type of a {@link Property}.
 * @author jnelson
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface DataType {
	String value() default "";
}
