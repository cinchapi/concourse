/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse;

import java.io.File;

import org.apache.thrift.transport.TTransportException;
import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.time.Time;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Throwables;

/**
 * This is the base class for all integration tests. This class contains logic
 * to setup a new {@link #server} and a corresponding {@link #client} connection
 * before every test. At the end of each test, those resources are cleaned up.
 * <p>
 * Interaction with the server goes through the {@link #client} variable.
 * </p>
 * 
 * @author jnelson
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
            System.out.println("TEST FAILURE in " + description.getMethodName()
                    + ": " + t.getMessage());
            System.out.println("---");
            System.out.println(Variables.dump());
            System.out.println("");
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
        server.grant(username.getBytes(), password.getBytes());
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
            server = new ConcourseServer(SERVER_PORT, SERVER_BUFFER_DIRECTORY,
                    SERVER_DATABASE_DIRECTORY);
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
