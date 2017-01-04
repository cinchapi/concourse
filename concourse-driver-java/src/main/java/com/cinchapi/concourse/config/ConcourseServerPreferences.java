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
package com.cinchapi.concourse.config;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import ch.qos.logback.classic.Level;

/**
 * A wrapper around the {@code concourse.prefs} file that is used to
 * configure the server.
 * <p>
 * Instantiate using {@link ConcourseServerPreferences#open(String)}
 * </p>
 * 
 * @author jnelson
 */
public class ConcourseServerPreferences extends PreferencesHandler {

    /**
     * Return a {@link ConcourseServerPreferences} handler that is backed by the
     * configuration information in {@code file}.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @return the preferences handler
     */
    public static ConcourseServerPreferences open(String file) {
        try {
            return new ConcourseServerPreferences(file);
        }
        catch (ConfigurationException e) {
            throw Throwables.propagate(e);
        }
    }

    static {
        // Prevent logging from showing up in the console
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ConcourseServerPreferences.class))
                .setLevel(Level.OFF);
    }

    // Defaults
    private static String DEFAULT_DATA_HOME = System.getProperty("user.home")
            + File.separator + "concourse" + File.separator;
    private static String DEFAULT_BUFFER_DIRECTORY = DEFAULT_DATA_HOME
            + "buffer";
    private static String DEFAULT_DATABASE_DIRECTORY = DEFAULT_DATA_HOME + "db";
    private static int DEFAULT_BUFFER_PAGE_SIZE = 8192;
    private static Level DEFAULT_LOG_LEVEL = Level.INFO;
    private static boolean DEFAULT_ENABLE_CONSOLE_LOGGING = false;
    private static int DEFAULT_CLIENT_PORT = 1717;
    private static int DEFAULT_SHUTDOWN_PORT = 3434;
    private static int DEFAULT_JMX_PORT = 9010;
    private static String DEFAULT_DEFAULT_ENVIRONMENT = "default";

    /**
     * Construct a new instance.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @throws ConfigurationException
     */
    private ConcourseServerPreferences(String file)
            throws ConfigurationException {
        super(file);
    }

    /**
     * The absolute path to the directory where the Buffer data is stored.
     * For optimal write performance, the Buffer should be placed on a
     * separate disk partition (ideally a separate physical device) from
     * the database_directory.
     * 
     * @return the buffer directory
     */
    public String getBufferDirectory() {
        return getString("buffer_directory", DEFAULT_BUFFER_DIRECTORY);
    }

    /**
     * The size for each page in the Buffer. During reads, Buffer pages
     * are individually locked, so it is desirable to have several smaller
     * pages as opposed to few larger ones. Nevertheless, be sure to balance
     * the desire to maximize lock granularity with the risks of having too
     * many open buffer files simultaneously.
     * 
     * @return the buffer page size in bytes
     */
    public long getBufferPageSize() {
        return getSize("buffer_page_size", DEFAULT_BUFFER_PAGE_SIZE);
    }

    /**
     * The listener port (1-65535) for client connections. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     * 
     * @return the client port
     */
    public int getClientPort() {
        return getInt("client_port", DEFAULT_CLIENT_PORT);
    }

    /**
     * The absolute path to the directory where the Database record and index
     * files are stored. For optimal performance, the Database should be
     * placed on a separate disk partition (ideally a separate physical device)
     * from the buffer_directory.
     * 
     * @return the database directory
     */
    public String getDatabaseDirectory() {
        return getString("database_directory", DEFAULT_DATABASE_DIRECTORY);
    }

    /**
     * The default environment that is automatically loaded when the server
     * starts and is used whenever a client does not specify an environment for
     * its connection.
     * 
     * @return the default environment
     */
    public String getDefaultEnvironment() {
        return getString("default_environment", DEFAULT_DEFAULT_ENVIRONMENT);
    }

    /**
     * Determine whether log messages should also be printed to the console
     * (STDOUT).
     * 
     * @return the determination whether to enable console logging
     */
    public boolean getEnableConsoleLogging() {
        return getBoolean("enable_console_logging",
                DEFAULT_ENABLE_CONSOLE_LOGGING);
    }

    /**
     * The listener port (1-65535) for management commands via JMX. Choose a
     * port between 49152 and 65535 to minimize the possibility of conflicts
     * with other services on this host.
     * 
     * @return the jmx port
     */
    public int getJmxPort() {
        return getInt("jmx_port", DEFAULT_JMX_PORT);
    }

    /**
     * # The amount of runtime information logged by the system. The options
     * below
     * are listed from least to most verbose. In addition to the indicated types
     * of information, each level also logs the information for each less
     * verbose
     * level (i.e. ERROR only prints error messages, but INFO prints info, warn
     * and error messages).
     * 
     * <p>
     * <strong>ERROR:</strong> critical information when the system reaches a
     * potentially fatal state and may not operate normally.
     * </p>
     * <p>
     * <strong>WARN:</strong> useful information when the system reaches a less
     * than ideal state but can continue to operate normally.
     * </p>
     * <p>
     * <strong>INFO:</strong> status information about the system that can be
     * used for sanity checking.
     * </p>
     * <p>
     * <strong>DEBUG:</strong> detailed information about the system that can be
     * used to diagnose bugs.
     * </p>
     * 
     * <p>
     * Logging is important, but may cause performance degradation. Only use the
     * DEBUG level for staging environments or instances when detailed
     * information is needed to diagnose a bug. Otherwise use the WARN or INFO
     * levels.
     * </p>
     * 
     * @return the log level
     */
    public Level getLogLevel() {
        return Level.valueOf(getString("log_level",
                DEFAULT_LOG_LEVEL.toString()));
    }

    /**
     * The listener port (1-65535) for shutdown commands. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     * 
     * @return the shutdown port
     */
    public int getShutdownPort() {
        return getInt("shutdown_port", DEFAULT_SHUTDOWN_PORT);
    }

    /**
     * Set the value associated with the {@code buffer_directory} key.
     * 
     * @param bufferDirectory
     */
    public void setBufferDirectory(String bufferDirectory) {
        setProperty("buffer_directory", bufferDirectory);
    }

    /**
     * Set the value associated with the {@code buffer_page_size} key.
     */
    public void setBufferPageSize(long sizeInBytes) {
        setProperty("buffer_page_size", sizeInBytes);
    }

    /**
     * Set the value associated with the {@code client_port} key.
     * 
     * @param clientPort
     */
    public void setClientPort(int clientPort) {
        setProperty("client_port", clientPort);
    }

    /**
     * Set the value associated with the {@code database_directory} key.
     * 
     * @param databaseDirectory
     */
    public void setDatabaseDirectory(String databaseDirectory) {
        setProperty("database_directory", databaseDirectory);
    }

    /**
     * Set the value associated with the {@code default_environment} key.
     * 
     * @param defaultEnvironment
     */
    public void setDefaultEnvironment(String defaultEnvironment) {
        setProperty("default_environment", defaultEnvironment);
    }

    /**
     * Set the value associated with the {@code enable_console_logging} key.
     * 
     * @param enableConsoleLogging
     */
    public void setEnableConsoleLogging(boolean enableConsoleLogging) {
        setProperty("enable_console_logging", enableConsoleLogging);
    }

    /**
     * Set the value associated with the {@code jmx_port} key.
     * 
     * @param port
     */
    public void setJmxPort(int port) {
        setProperty("jmx_port", port);
    }

    /**
     * Set the value associated with the {@code log_level} key.
     * 
     * @param level
     */
    public void setLogLevel(Level level) {
        setProperty("log_level", level.toString());
    }

    /**
     * Set the value associated with the {@code shutdown_port} key.
     * 
     * @param shutdownPort
     */
    public void setShutdownPort(int shutdownPort) {
        setProperty("shutdown_port", shutdownPort);
    }

}
