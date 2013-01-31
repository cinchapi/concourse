package com.cinchapi.concourse.model;

import com.google.common.primitives.UnsignedLong;

/**
 * A set of record ids sorted in reverse chronological order.
 * 
 * @author jnelson
 */
class RecordSet extends TimeSortedSet<UnsignedLong> {

	@Override
	protected TimeSortableObject<UnsignedLong> createObject(UnsignedLong object,
			UnsignedLong timestamp){
		return new Record(object, timestamp);
	}

	/**
	 * Encapsulates a record id for the sake of time sortability.
	 */
	class Record extends TimeSortableObject<UnsignedLong> {

		/**
		 * Construct a new Record instance.
		 * 
		 * @param object
		 * @param timestamp
		 */
		public Record(UnsignedLong object, UnsignedLong timestamp) {
			super(object, timestamp);
		}

	}

}
