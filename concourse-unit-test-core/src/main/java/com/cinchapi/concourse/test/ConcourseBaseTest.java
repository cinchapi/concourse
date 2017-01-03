/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.test;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.test.Variables;

/**
 * The base class for all Concourse tests. ALL Concourse tests should extend
 * this class to ensure that they have proper error handling, cleanup, etc.
 * <p>
 * Each subclass should register local variables using
 * {@link Variables#register(String, Object)} to ensure that they are printed on
 * test failure to help with debugging.
 * 
 * <pre>
 * long number = Variables.register(&quot;number&quot;, TestData.getLong());
 * Assert.assertNotEquals(number, (long) Variables.get(&quot;smaller_number&quot;));
 * </pre>
 * 
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class ConcourseBaseTest {

    // Initialization for all tests
    static {
        System.setProperty("test", "true");
    }

    /**
     * This watcher clears previously registered {@link Variables} on startup
     * and dumps them in the event of failure.
     */
    @Rule
    public final TestWatcher __watcher = new TestWatcher() {

        @Override
        protected void failed(Throwable t, Description description) {
            System.err.println("TEST FAILURE in " + description.getMethodName()
                    + ": " + t.getMessage());
            System.err.println("---");
            System.err.println(Variables.dump());
            System.err.println("");
        }

        @Override
        protected void finished(Description description) {
            afterEachTest();
        }

        @Override
        protected void starting(Description description) {
            Variables.clear();
            beforeEachTest();
        }

    };

    /**
     * This method is provided for the subclass to specify additional behaviour
     * to be run after each test is done. The subclass should define such logic
     * in this method as opposed to a test watcher.
     */
    protected void afterEachTest() {}

    /**
     * This method is provided for the subclass to specify additional behaviour
     * to be run before each test begins. The subclass should define such logic
     * in this method as opposed to a test watcher.
     */
    protected void beforeEachTest() {}

}
