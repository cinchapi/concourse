package com.cinchapi.concourse.data;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

;

@RunWith(Suite.class)
@SuiteClasses({
	IntegerPropertyTest.class,
	LongPropertyTest.class,
	StringPropertyTest.class
})
public class PropertyTestSuite { }
