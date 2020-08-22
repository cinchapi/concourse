/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.util.concurrent.CountDownLatch;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.Environments;
import com.cinchapi.concourse.util.Networking;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link com.cinchapi.concourse.server.ConcourseServer}.
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

    @Test
    public void testGetEngineRaceCondition()
            throws TException, InterruptedException { // CON-673
        int port = Networking.getOpenPort();
        String env = "test";
        String buffer = TestData.getTemporaryTestDir();
        String db = TestData.getTemporaryTestDir();
        server = ConcourseServer.create(port, buffer, db);
        server.spawn();
        try {
            AccessToken token = server.login(
                    ByteBuffers.fromUtf8String("admin"),
                    ByteBuffers.fromUtf8String("admin"), env);
            for (int i = 0; i < 10000; ++i) {
                server.addKeyValue(TestData.getSimpleString(),
                        TestData.getTObject(), token, null, env);
            }
            server.stop();
            server = ConcourseServer.create(port, buffer, db);
            server.spawn();
            int threads = 20;
            CountDownLatch latch = new CountDownLatch(threads);
            for (int i = 0; i < threads; ++i) {
                Thread t = new Thread(() -> {
                    Concourse client = Concourse.at().port(port)
                            .environment(env).connect();
                    client.exit();
                    latch.countDown();
                });
                t.start();
            }
            latch.await();
            Assert.assertEquals(2, server.numEnginesInitialized.get());
        }
        finally {
            server.stop();
        }
    }

}
