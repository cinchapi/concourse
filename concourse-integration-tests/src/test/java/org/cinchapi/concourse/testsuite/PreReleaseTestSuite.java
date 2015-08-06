/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cinchapi.concourse.testsuite;

import org.cinchapi.concourse.demo.GettingStartedTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * ALWAYS RUN THIS TEST BEFORE RELEASING A NEW VERSION.
 * 
 * @author Jeff Nelson
 */
@RunWith(Suite.class)
@SuiteClasses({ GettingStartedTest.class, IntegrationTestSuite.class,
        BugReproSuite.class })
public class PreReleaseTestSuite {

}
