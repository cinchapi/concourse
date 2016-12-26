/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.ConcourseCodebase;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.ManagedConcourseServer;
import com.cinchapi.concourse.server.ManagedConcourseServer.ReflectiveClient;

/**
 * Unit tests for {@link ManagedConcourseServer}.
 * 
 * @author jnelson
 */
public class ManagedConcourseServerTest {

    private ManagedConcourseServer server = null;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            ConcourseCodebase codebase = ConcourseCodebase.cloneFromGithub();
            String installer = codebase.buildInstaller();
            server = ManagedConcourseServer
                    .manageNewServer(Paths.get(installer).toFile());
        }

        @Override
        protected void finished(Description description) {
            server.destroy();
            server = null;
        }
    };

    @Test
    public void testStart() {
        server.start();
        Assert.assertTrue(server.isRunning());
    }

    @Test
    public void testStop() {
        server.start();
        server.stop();
        Assert.assertFalse(server.isRunning());
    }

    @Test
    public void testStopWithClient() {
        server.start();
        server.connect();
        server.stop();
        Assert.assertFalse(server.isRunning());
    }

    @Test
    public void testIsRunning() {
        Assert.assertFalse(server.isRunning());
        server.start();
        Assert.assertTrue(server.isRunning());
    }

    @Test
    public void testDestroy() {
        server.destroy();
        Assert.assertFalse(
                Files.exists(Paths.get(server.getInstallDirectory())));
    }

    @Test
    public void testConnectAndInteract() {
        server.start();
        Concourse concourse = server.connect();
        concourse.add("foo", "bar", 1);
        Assert.assertEquals("bar", concourse.get("foo", 1));
    }

    @Test
    public void testClientFind() {
        server.start();
        Concourse concourse = server.connect();
        concourse.add("foo", 1, 1);
        Assert.assertTrue(
                concourse.find("foo", Operator.EQUALS, 1).contains(1L));
    }

    @Test
    public void testClientFindWithTime() {
        server.start();
        Concourse concourse = server.connect();
        concourse.add("foo", 1, 1);
        Timestamp timestamp = Timestamp.now();
        concourse.add("foo", 1, 2);
        Assert.assertFalse(concourse.find("foo", Operator.EQUALS, 1, timestamp)
                .contains(2L));
    }

    @Test
    public void testAutoUnboxOnClientCall() {
        server.start();
        ReflectiveClient concourse = (ReflectiveClient) server.connect();
        concourse.add("name", "jeff", 1);
        long record = 1;
        String result = concourse.call("get", "name", record);
        Assert.assertEquals("jeff", result);
    }

    @Test
    public void testExecuteCli() {
        server.start();
        List<String> stdout = server.executeCli("users",
                "--list-sessions --username admin --password admin");
        boolean passed = false;
        for (String line : stdout) {
            if(line.contains("Current User Sessions")) {
                passed = true;
                break;
            }
        }
        Assert.assertTrue(passed);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCalculator() {
        server.start();
        Concourse concourse = server.connect();
        concourse.add("age", 20);
        concourse.add("age", 40);
        Assert.assertEquals(60, concourse.calculate().sum("age"));
    }

}
