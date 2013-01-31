package com.cinchapi.concourse.model;

import com.google.common.primitives.UnsignedLong;

/**
 * A set of values sorted in reverse chronological order.
 * 
 * @author jnelson
 */
class ValueSet extends TimeSortedSet<Object> {

	@Override
	protected TimeSortableObject<Object> createObject(Object object,
			UnsignedLong timestamp){
		return new Value(object, timestamp);
	}

	/**
	 * Encapsulates a value for the sake of time sortability.
	 */
	class Value extends TimeSortableObject<Object> {

		/**
		 * Construct a new Value instance.
		 * 
		 * @param object
		 * @param timestamp
		 */
		public Value(Object object, UnsignedLong timestamp) {
			super(object, timestamp);
		}

	}

}
