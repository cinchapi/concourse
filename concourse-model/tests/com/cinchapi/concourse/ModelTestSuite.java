package com.cinchapi.concourse;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cinchapi.concourse.data.PropertyTestSuite;
import com.cinchapi.concourse.model.DefaultPropertyRecordTest;

@RunWith(Suite.class)
@SuiteClasses({
	//SimpleMetadataTest.class,
	DefaultPropertyRecordTest.class,
	PropertyTestSuite.class
})
public class ModelTestSuite {
	
	
}
