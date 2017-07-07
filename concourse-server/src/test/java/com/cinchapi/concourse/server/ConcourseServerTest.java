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
package com.cinchapi.concourse.server;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.Environments;

/**
 * Unit tests for {@link ConcourseServer}.
 * 
 * @author Jeff Nelson
 */
public class ConcourseServerTest extends ConcourseBaseTest {

    /**
     * A reference to a ConcourseServer instance that can be used in each unit
     * test.
     */
    protected ConcourseServer server;

    @Test(expected = IllegalStateException.class)
    public void testCannotStartServerWhenBufferAndDatabaseDirectoryAreSame()
            throws TTransportException {
        ConcourseServer.create(1, System.getProperty("user.home"),
                System.getProperty("user.home"));
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotStartServerWhenDefaultEnvironmentIsEmptyString()
            throws TTransportException, MalformedObjectNameException,
            InstanceAlreadyExistsException, MBeanRegistrationException,
            NotCompliantMBeanException {
        String oldDefault = GlobalState.DEFAULT_ENVIRONMENT;
        try {
            GlobalState.DEFAULT_ENVIRONMENT = "$$";
            ConcourseServer.create(1, "buffer", "db");
        }
        finally {
            GlobalState.DEFAULT_ENVIRONMENT = oldDefault;
        }
    }

    @Test
    public void testFindEnvReturnsDefaultForEmptyString() {
        Assert.assertEquals(GlobalState.DEFAULT_ENVIRONMENT,
                Environments.sanitize(""));
    }

    @Test
    public void testFindEnvStripsNonAlphaNumChars() {
        String env = "$%&foo@3**";
        Assert.assertEquals("foo3", Environments.sanitize(env));
    }

    @Test
    public void testFindEnvStripsNonAlphaNumCharsInDefaultEnv() {
        String oldDefault = GlobalState.DEFAULT_ENVIRONMENT;
        GlobalState.DEFAULT_ENVIRONMENT = "%$#9blah@@3foo1#$";
        Assert.assertEquals("9blah3foo1", Environments.sanitize(""));
        GlobalState.DEFAULT_ENVIRONMENT = oldDefault;
    }

    @Test
    public void testFindEnvKeepsUnderScore() {
        String env = "$_%&test_@envir==--onment*_*";
        Assert.assertEquals("_test_environment_", Environments.sanitize(env));
    }

}
