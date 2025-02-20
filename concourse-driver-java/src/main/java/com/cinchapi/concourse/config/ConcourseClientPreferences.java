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

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.logging.Logging;

/**
 * A wrapper around the {@code concourse_client.prefs} file that is used to
 * configure a client connection.
 * <p>
 * Instantiate using {@link ConcourseClientPreferences#open(String)}
 * </p>
 * 
 * @author Jeff Nelson
 * @deprecated Use {@link ConcourseClientConfiguration} instead
 */
@Deprecated
public abstract class ConcourseClientPreferences extends PreferencesHandler {

    static {
        Logging.disable(ConcourseClientPreferences.class);
    }

    /**
     * Return a {@link ConcourseClientPreferences} handler that is sourced from
     * the {@link files}.
     * 
     * @param files
     * @return the handler
     */
    public static ConcourseClientPreferences from(Path... files) {
        Verify.thatArgument(files.length > 0, "Must include at least one file");
        return new ConcourseClientConfiguration(files);
    }

    /**
     * Return a {@link ConcourseClientPreferences} handler that is sourced from
     * a concourse_client.prefs file in the current working directory.
     * 
     * @return the handler
     */
    public static ConcourseClientPreferences fromCurrentWorkingDirectory() {
        return ConcourseClientConfiguration.fromCurrentWorkingDirectory();
    }

    /**
     * Return a {@link ConcourseClientPreferences} handler that is sourced from
     * a concourse_client.prefs file in the user's home directory.
     * 
     * @return the handler
     */
    public static ConcourseClientPreferences fromUserHomeDirectory() {
        return ConcourseClientConfiguration.fromUserHomeDirectory();
    }

    /**
     * Return a {@link ConcourseClientPreferences} wrapper that is backed by the
     * configuration information in {@code file}.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @return the preferences
     * @deprecated use {@link ConcourseServerPreferences#from(Path...)} instead
     */
    @Deprecated
    public static ConcourseClientPreferences open(String file) {
        return from(Paths.get(file));
    }

    /**
     * Construct a new instance.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     */
    ConcourseClientPreferences(Path... files) {
        super(files);
    }

    /**
     * Return the value associated with the {@code environment} key.
     * 
     * @return the environment
     */
    public abstract String getEnvironment();

    /**
     * Return the value associated with the {@code host} key.
     * 
     * @return the host
     */
    public abstract String getHost();

    /**
     * Return the value associated with the {@code password} key.
     * <p>
     * <strong>NOTE</strong>: This method returns the password as a char array
     * so that the caller can null out the data immediately after use. This is
     * generally advised to limit the amount of time that the sensitive data
     * remains in memory.
     * </p>
     * 
     * @return the password
     */
    public abstract char[] getPassword();

    /**
     * Return the value associated with the {@code password} key, if it is
     * explicitly defined in the prefs file. Unlike the {@link #getPassword()}
     * method, this one will not return the default password if one is not
     * explicitly defined in the file.
     * <p>
     * <strong>NOTE</strong>: This method returns the password as a char array
     * so that the caller can null out the data immediately after use. This is
     * generally advised to limit the amount of time that the sensitive data
     * remains in memory.
     * </p>
     * 
     * @return the password or an empty character array if it is not defined
     */
    public abstract char[] getPasswordExplicit();

    /**
     * Return the value associated with the {@code port} key.
     * 
     * @return the port
     */
    public abstract int getPort();

    /**
     * Return the value associated with the {@code username} key.
     * 
     * @return the username
     */
    public abstract String getUsername();

    /**
     * Set the value associated with the {@code environment} key.
     * 
     * @param environment
     */
    public abstract void setEnvironment(String environment);

    /**
     * Set the value associated with the {@code host} key.
     * 
     * @param host
     */
    public abstract void setHost(String host);

    /**
     * Set the value associated with the {@code password} key.
     * 
     * @param password
     */
    public abstract void setPassword(char[] password);

    /**
     * Set the value associated with the {@code port} key.
     * 
     * @param port
     */
    public abstract void setPort(int port);

    /**
     * Set the value associated with the {@code username} key.
     * 
     * @param username
     */
    public abstract void setUsername(String username);
}