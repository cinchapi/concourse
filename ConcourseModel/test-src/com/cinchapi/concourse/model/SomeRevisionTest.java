package com.cinchapi.concourse.model;

import org.joda.time.DateTime;

public class SomeRevisionTest extends RevisionTest{

	@Override
	protected <T> Revision<T> revision(Thing thing, String key, T value,
			DateTime timestamp){
		return new SomeRevision<T>(thing, key, value, timestamp);
	}

}
