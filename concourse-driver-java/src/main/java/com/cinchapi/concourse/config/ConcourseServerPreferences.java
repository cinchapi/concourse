/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.nio.file.Path;
import java.nio.file.Paths;

import ch.qos.logback.classic.Level;

import com.cinchapi.common.base.Verify;
import com.cinchapi.concourse.util.Logging;

/**
 * A wrapper around the {@code concourse.prefs} file that is used to
 * configure the server.
 * <p>
 * Instantiate using {@link ConcourseServerPreferences#open(String)}
 * 
 * @author Jeff Nelson
 * @deprecated Use {@link ConcourseServerConfiguration} instead
 */
@Deprecated
public abstract class ConcourseServerPreferences extends PreferencesHandler {

    /**
     * Return a {@link ConcourseServerPreferences} handler that is backed by the
     * configuration information in {@code file}.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @return the preferences handler
     * @deprecated use {@link ConcourseServerPreferences#from(Path...)} instead
     */
    @Deprecated
    public static ConcourseServerPreferences open(String file) {
        return from(Paths.get(file));

    }

    /**
     * Return a {@link ConcourseServerPreferences} handler that is sourced from
     * the {@link files}.
     * 
     * @param files
     * @return the sources
     */
    public static ConcourseServerPreferences from(Path... files) {
        Verify.thatArgument(files.length > 0, "Must include at least one file");
        return new ConcourseServerConfiguration(files);
    }

    static {
        Logging.disable(ConcourseServerPreferences.class);
    }

    /**
     * Construct a new instance.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @throws ConfigurationException
     */
    ConcourseServerPreferences(Path... files) {
        super(files);
    }

    /**
     * The absolute path to the directory where the Buffer data is stored.
     * For optimal write performance, the Buffer should be placed on a
     * separate disk partition (ideally a separate physical device) from
     * the database_directory.
     * 
     * @return the buffer directory
     */
    public abstract String getBufferDirectory();

    /**
     * The size for each page in the Buffer. During reads, Buffer pages
     * are individually locked, so it is desirable to have several smaller
     * pages as opposed to few larger ones. Nevertheless, be sure to balance
     * the desire to maximize lock granularity with the risks of having too
     * many open buffer files simultaneously.
     * 
     * @return the buffer page size in bytes
     */
    public abstract long getBufferPageSize();

    /**
     * The listener port (1-65535) for client connections. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     * 
     * @return the client port
     */
    public abstract int getClientPort();

    /**
     * The absolute path to the directory where the Database record and index
     * files are stored. For optimal performance, the Database should be
     * placed on a separate disk partition (ideally a separate physical device)
     * from the buffer_directory.
     * 
     * @return the database directory
     */
    public abstract String getDatabaseDirectory();

    /**
     * The default environment that is automatically loaded when the server
     * starts and is used whenever a client does not specify an environment for
     * its connection.
     * 
     * @return the default environment
     */
    public abstract String getDefaultEnvironment();

    /**
     * Determine whether log messages should also be printed to the console
     * (STDOUT).
     * 
     * @return the determination whether to enable console logging
     */
    public abstract boolean getEnableConsoleLogging();

    /**
     * The listener port (1-65535) for management commands via JMX. Choose a
     * port between 49152 and 65535 to minimize the possibility of conflicts
     * with other services on this host.
     * 
     * @return the jmx port
     */
    public abstract int getJmxPort();

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
    public abstract Level getLogLevel();

    /**
     * The listener port (1-65535) for shutdown commands. Choose a port between
     * 49152 and 65535 to minimize the possibility of conflicts with other
     * services on this host.
     * 
     * @return the shutdown port
     */
    public abstract int getShutdownPort();

    /**
     * Set the value associated with the {@code buffer_directory} key.
     * 
     * @param bufferDirectory
     */
    public abstract void setBufferDirectory(String bufferDirectory);

    /**
     * Set the value associated with the {@code buffer_page_size} key.
     */
    public abstract void setBufferPageSize(long sizeInBytes);

    /**
     * Set the value associated with the {@code client_port} key.
     * 
     * @param clientPort
     */
    public abstract void setClientPort(int clientPort);

    /**
     * Set the value associated with the {@code database_directory} key.
     * 
     * @param databaseDirectory
     */
    public abstract void setDatabaseDirectory(String databaseDirectory);

    /**
     * Set the value associated with the {@code default_environment} key.
     * 
     * @param defaultEnvironment
     */
    public abstract void setDefaultEnvironment(String defaultEnvironment);

    /**
     * Set the value associated with the {@code enable_console_logging} key.
     * 
     * @param enableConsoleLogging
     */
    public abstract void setEnableConsoleLogging(boolean enableConsoleLogging);

    /**
     * Set the value associated with the {@code jmx_port} key.
     * 
     * @param port
     */
    public abstract void setJmxPort(int port);

    /**
     * Set the value associated with the {@code log_level} key.
     * 
     * @param level
     */
    public abstract void setLogLevel(Level level);

    /**
     * Set the value associated with the {@code shutdown_port} key.
     * 
     * @param shutdownPort
     */
    public abstract void setShutdownPort(int shutdownPort);

}
