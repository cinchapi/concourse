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
package com.cinchapi.concourse.test;

import java.io.File;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.server.ConcourseServer;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.time.Time;
import com.google.common.base.Throwables;

/**
 * This is the base class for all integration tests. This class contains logic
 * to setup a new {@link #server} and a corresponding {@link #client} connection
 * before every test. At the end of each test, those resources are cleaned up.
 * <p>
 * Interaction with the server goes through the {@link #client} variable.
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class ConcourseIntegrationTest {

    // Initialization for all tests
    static {
        System.setProperty("test", "true");
    }

    /**
     * The tests run against a local server.
     */
    protected static final String SERVER_HOST = "localhost";

    /**
     * The default server port is 1717, so we use 1718 as to avoid interfering
     * with any real servers that might be running.
     */
    protected static final int SERVER_PORT = 1718;

    /**
     * The test server stores data in a distinct folder under the user's home
     * directory. This directory is deleted after each test.
     */
    private static final String SERVER_DATA_HOME = System
            .getProperty("user.home")
            + File.separator
            + "concourse_"
            + Long.toString(Time.now());
    private static final String SERVER_DATABASE_DIRECTORY = SERVER_DATA_HOME
            + File.separator + "db";
    private static final String SERVER_BUFFER_DIRECTORY = SERVER_DATA_HOME
            + File.separator + "buffer";

    /**
     * The instance of the local server that is running. The subclass should not
     * need to access this directly because all calls should be funneled through
     * the {@link #client}.
     */
    private ConcourseServer server;

    /**
     * The client that is used to interact with the server.
     */
    protected Concourse client;

    @Rule
    public TestWatcher __watcher = new TestWatcher() {

        @Override
        protected void failed(Throwable t, Description description) {
            System.err.println("TEST FAILURE in " + description.getMethodName()
                    + ": " + t.getMessage());
            System.err.println("---");
            System.err.println(Variables.dump());
            System.err.println("");
            stop();
            afterEachTest();
        }

        @Override
        protected void finished(Description description) {
            stop();
            afterEachTest();
        }

        @Override
        protected void starting(Description description) {
            Variables.clear();
            start();
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

    /**
     * Grant access to the server for a user identified by {@code username} and
     * {@code password}.
     * 
     * @param username
     * @param password
     */
    protected final void grantAccess(String username, String password) {
        try {
            AccessToken token = server.login(
                    ByteBuffers.fromUtf8String("admin"),
                    ByteBuffers.fromUtf8String("admin"));
            server.grant(ByteBuffers.fromUtf8String(username),
                    ByteBuffers.fromUtf8String(password), token);
        }
        catch (TException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Disable access to the server for the user identified by {@code username}.
     * 
     * @param username the username for which access should be disabled
     */
    protected final void disableAccess(String username) {
        try {
            AccessToken token = server.login(
                    ByteBuffers.fromUtf8String("admin"),
                    ByteBuffers.fromUtf8String("admin"));
            server.disableUser(ByteBuffers.fromUtf8String(username), token);
        }
        catch (TException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Reset the test by stopping the server, deleting any stored data, and
     * starting a new server.
     */
    protected void reset() {
        stop();
        start();
    };

    /**
     * Restart the embedded server. This method will preserve stored data.
     */
    protected void restartServer() {
        server.stop();
        start();
    }

    /**
     * Startup a new {@link ConcourseServer} and grab a new client connection.
     */
    private void start() {
        startServer();
        client = Concourse.connect(SERVER_HOST, SERVER_PORT, "admin", "admin");
    }

    /**
     * Start an embedded server.
     */
    private void startServer() {
        try {
            server = ConcourseServer.create(SERVER_PORT,
                    SERVER_BUFFER_DIRECTORY, SERVER_DATABASE_DIRECTORY);
        }
        catch (TTransportException e1) {
            throw Throwables.propagate(e1);
        }
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    server.start();
                }
                catch (TTransportException e) {
                    throw Throwables.propagate(e);
                }

            }

        });
        t.start();
    };

    /**
     * Exit the client. Stop the server. Delete any stored data.
     */
    private void stop() {
        client.exit();
        server.stop();
        FileSystem.deleteDirectory(SERVER_DATA_HOME);
        FileSystem.deleteFile(".access"); // delete the creds in case there were
                                          // any changes made during a test
    }

}
