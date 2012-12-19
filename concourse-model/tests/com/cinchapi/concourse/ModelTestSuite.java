package com.cinchapi.concourse;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cinchapi.concourse.model.SimpleMetadataTest;
import com.cinchapi.concourse.model.SimplePropertyRecordTest;
import com.cinchapi.concourse.property.PropertyTestSuite;

@RunWith(Suite.class)
@SuiteClasses({
	//SimpleMetadataTest.class,
	SimplePropertyRecordTest.class,
	PropertyTestSuite.class
})
public class ModelTestSuite {
	
	
}
