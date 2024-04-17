/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.io.File;
import java.lang.management.MemoryUsage;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;

import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.config.ConcourseServerConfiguration;
import com.cinchapi.concourse.config.ConcourseServerPreferences;

/**
 * A {@link ManagedConcourseServer} is an external server process that can be
 * programmatically controlled within another application. This class is useful
 * for applications that want to "embed" a Concourse Server for the duration of
 * the application's life cycle and then forget about its existence afterwards.
 * 
 * @author jnelson
 * @deprecated use
 *             {@link com.cinchapi.concourse.automation.server.ManagedConcourseServer}
 *             instead
 */
@Deprecated
public class ManagedConcourseServer {

    /**
     * Return an {@link ManagedConcourseServer} that controls an instance
     * located in the {@code installDirectory}.
     * 
     * @param installDirectory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageExistingServer(
            String installDirectory) {
        return new ManagedConcourseServer(
                com.cinchapi.concourse.automation.server.ManagedConcourseServer
                        .open(Paths.get(installDirectory)));
    }

    /**
     * Create an {@link ManagedConcourseServer} from the {@code installer}.
     * 
     * @param installer
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(File installer) {
        return new ManagedConcourseServer(
                com.cinchapi.concourse.automation.server.ManagedConcourseServer
                        .install(installer.toPath()));
    }

    /**
     * Create an {@link ManagedConcourseServer} from the {@code installer} in
     * {@code directory}.
     * 
     * @param installer
     * @param directory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(File installer,
            String directory) {
        return new ManagedConcourseServer(
                com.cinchapi.concourse.automation.server.ManagedConcourseServer
                        .install(installer.toPath(), Paths.get(directory)));
    }

    /**
     * Create an {@link ManagedConcourseServer} at {@code version}.
     * 
     * @param version
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(String version) {
        return new ManagedConcourseServer(
                com.cinchapi.concourse.automation.server.ManagedConcourseServer
                        .install(version));
    }

    /**
     * Create an {@link ManagedConcourseServer} at {@code version} in
     * {@code directory}.
     * 
     * @param version
     * @param directory
     * @return the ManagedConcourseServer
     */
    public static ManagedConcourseServer manageNewServer(String version,
            String directory) {
        return new ManagedConcourseServer(
                com.cinchapi.concourse.automation.server.ManagedConcourseServer
                        .install(version, Paths.get(directory)));
    }

    /**
     * The delegate instance to which functionality is forwaded.
     */
    private final com.cinchapi.concourse.automation.server.ManagedConcourseServer delegate;

    /**
     * Construct a new instance.
     * 
     * @param installDirectory
     */
    private ManagedConcourseServer(
            com.cinchapi.concourse.automation.server.ManagedConcourseServer delegate) {
        this.delegate = delegate;
    }

    /**
     * Return the {@link ManagedConcourseServer server's}
     * {@link ConourseServerConfiguration configuration}.
     * 
     * @return the {@link ConourseServerPreferences configuration}.
     */
    public ConcourseServerConfiguration config() {
        return delegate.config();
    }

    /**
     * Return a connection handler to the server using the default "admin"
     * credentials.
     * 
     * @return the connection handler
     */
    public Concourse connect() {
        return delegate.connect();
    }

    /**
     * Return a connection handler to the server using the specified
     * {@code username} and {@code password}.
     * 
     * @param username
     * @param password
     * @return the connection handler
     */
    public Concourse connect(String username, String password) {
        return delegate.connect(username, password);
    }

    public Concourse connect(String username, String password,
            String environment) {
        return delegate.connect(username, password, environment);
    }

    /**
     * Stop the server, if it is running, and permanently delete the application
     * files and associated data.
     */
    public void destroy() {
        delegate.destroy();
    }

    /**
     * Return {@code true} if this server should be destroyed when the JVM
     * exits.
     * 
     * @return whether the server should be destroyed or not when the JVM exits
     */
    public synchronized boolean destroyOnExit() {
        return delegate.destroyOnExit();
    }

    /**
     * Set a flag that determines whether this instance will be destroyed on
     * exit.
     * 
     * @param destroyOnExit
     */
    public synchronized void destroyOnExit(boolean destroyOnExit) {
        delegate.setDestroyOnExit(destroyOnExit);
    }

    /**
     * Execute the specified {@code cli} with the provided {@code args}.
     * <p>
     * This is the equivalent of calling {@code concourse <cli> <args>}
     * on the command line
     * </p>
     * 
     * @param cli the name of the CLI to execute
     * @param args the args to pass to the cli
     * @return the standard output from executing the cli
     */
    public List<String> executeCli(String cli, String... args) {
        return delegate.executeCli(cli, args);
    }

    /**
     * Return the directory where the server stores buffer files.
     * 
     * @return the buffer directory
     */
    public Path getBufferDirectory() {
        return delegate.getBufferDirectory();
    }

    /**
     * Return the client port for this server.
     * 
     * @return the client port
     */
    public int getClientPort() {
        return delegate.getClientPort();
    }

    /**
     * Return the directory where the server stores database files.
     * 
     * @return the database directory
     */
    public Path getDatabaseDirectory() {
        return delegate.getDatabaseDirectory();
    }

    /**
     * Get a collection of stats about the heap memory usage for the managed
     * concourse-server process.
     * 
     * @return the heap memory usage
     */
    public MemoryUsage getHeapMemoryStats() {
        return delegate.getHeapMemoryStats();
    }

    /**
     * Return the {@link #installDirectory} for this server.
     * 
     * @return the install directory
     */
    public String getInstallDirectory() {
        return delegate.directory().toString();
    }

    /**
     * Return the connection to the MBean sever of the managed concourse-server
     * process.
     * 
     * @return the mbean server connection
     */
    public MBeanServerConnection getMBeanServerConnection() {
        return delegate.getMBeanServerConnection();
    }

    /**
     * Get a collection of stats about the non heap memory usage for the managed
     * concourse-server process.
     * 
     * @return the non-heap memory usage
     */
    public MemoryUsage getNonHeapMemoryStats() {
        return delegate.getNonHeapMemoryStats();
    }

    /**
     * Return {@code true} if the default environment has writes that in its
     * buffer that are transportable to its database.
     * 
     * @return {@code true} if there are writes to transport in the default
     *         environment
     */
    public boolean hasWritesToTransport() {
        return delegate.hasWritesToTransport();
    }

    /**
     * Return {@code true} if the {@code environment} has writes that in its
     * buffer that are transportable to its database.
     * 
     * @return {@code true} if there are writes to transport in the
     *         {@code environment}
     */
    public boolean hasWritesToTransport(String environment) {
        return delegate.hasWritesToTransport(environment);
    }

    /**
     * Install the plugin(s) contained in the {@code bundle} on this
     * {@link ManagedConcourseServer}.
     * 
     * @param bundle the path to the plugin bundle
     * @return {@code true} if the plugin(s) from the bundle is/are installed
     */
    public boolean installPlugin(Path bundle) {
        return delegate.installPlugin(bundle);

    }

    /**
     * Return {@code true} if the server is currently running.
     * 
     * @return {@code true} if the server is running
     */
    public boolean isRunning() {
        return delegate.isRunning();
    }

    /**
     * Return the {@link ManagedConcourseServer server's}
     * {@link ConourseServerPreferences preferences}.
     * 
     * @return the {@link ConourseServerPreferences preferences}.
     * @deprecated use {@link #config() }instead
     */
    @Deprecated
    public ConcourseServerPreferences prefs() {
        return delegate.config();
    }

    /**
     * Print the content of the log file with {@code name} to the console.
     * 
     * @param name the name of the log file (i.e. console)
     */
    public void printLog(String name) {
        delegate.printLog(name);
    }

    /**
     * Print the content of the log files for each of the log {@code levels} to
     * the console.
     * 
     * @param levels the log levels to print
     */
    public void printLogs(LogLevel... levels) {
        delegate.printLogs(Arrays.stream(levels).map(
                level -> com.cinchapi.concourse.automation.server.ManagedConcourseServer.LogLevel
                        .valueOf(level.name()))
                .collect(Collectors.toList()).toArray(Array.containing()));
    }

    /**
     * Start the server.
     */
    public void start() {
        delegate.start();
    }

    /**
     * Stop the server.
     */
    public void stop() {
        delegate.stop();
    }

    /**
     * Copy the connection information for this managed server to a client
     * configuration file located in the root of the working directory so that
     * source code relying on the default connection behaviour will properly
     * connect to this server.
     * <p>
     * A test case that uses an indirect connection to Concourse (i.e. the test
     * case doesn't directly use the provided {@code client} variable provided
     * by the framework, but uses classes from the application source code that
     * has its own mechanism for connecting to Concourse) SHOULD call this
     * method so that the application code will connect to the this server for
     * the purpose of the unit test.
     * </p>
     * <p>
     * Any connection information that is synchronized will be cleaned up after
     * the test. If a prefs file already existed in the root of the working
     * directory, that file is backed up and restored so that the application
     * can run normally outside of the test cases.
     * </p>
     */
    public void syncDefaultClientConnectionInfo() {
        delegate.syncDefaultClientConnectionInfo();
    }

    /**
     * Enum for log levels that can be passed to the
     * {@link #printLogs(LogLevel...)} method
     * 
     * @author Jeff Nelson
     */
    public enum LogLevel {
        CONSOLE, DEBUG, ERROR, INFO, WARN
    }

}
