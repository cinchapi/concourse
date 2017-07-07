/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.cli;

import java.security.Permission;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link CommandLineInterfaceRunner}.
 * 
 * @author Jeff Nelson
 */
public class CommandLineInterfaceRunnerTest extends ConcourseIntegrationTest {

    /**
     * A flag that verifies that {@link #testDynamicParamters()} works.
     */
    private static AtomicBoolean DYNAMIC_PARAMS_EXIST = new AtomicBoolean(false);

    /**
     * A pointer to the original {@link SecurityManager}.
     */
    private SecurityManager sm = null;

    @Override
    public void afterEachTest() {
        System.setSecurityManager(sm);
    }

    @Override
    public void beforeEachTest() {
        sm = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {

            @Override
            public void checkExit(int status) {
                super.checkExit(status);
                throw new SystemExitInvoked();
            }

            @Override
            public void checkPermission(Permission perm) {
                // allow anything.
            }

            @Override
            public void checkPermission(Permission perm, Object context) {
                // allow anything.
            }

        });
    }

    @Test
    public void testDynamicParamters() {
        try {
            CommandLineInterfaceRunner.run(FakeCli.class,
                    "-Dfoo=bar -Dbaz=bang");
        }
        catch (SystemExitInvoked e) {}
        Assert.assertTrue(DYNAMIC_PARAMS_EXIST.get());
    }

    /**
     * A fake cli
     * 
     * @author Jeff Nelson
     */
    private static class FakeCli extends CommandLineInterface {

        /**
         * Construct a new instance.
         * 
         * @param args
         */
        public FakeCli(String[] args) {
            super(args);
        }

        @Override
        protected void doTask() {
            DYNAMIC_PARAMS_EXIST.set(!this.options.dynamic.isEmpty());
            DYNAMIC_PARAMS_EXIST.set(this.options.dynamic.get("foo").equals(
                    "bar"));
            DYNAMIC_PARAMS_EXIST.set(this.options.dynamic.get("baz").equals(
                    "bang"));
        }

        @Override
        protected Options getOptions() {
            return new Options();
        }

    }

    /**
     * A marker interface to indicate that {@link System#exit(int)} was called.
     * 
     * @author Jeff Nelson
     */
    @SuppressWarnings("serial")
    private static class SystemExitInvoked extends SecurityException {}

}
