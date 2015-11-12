/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.common.base.Throwables;

/**
 * A wrapper around the {@code concourse_client.prefs} file that is used to
 * configure a client connection.
 * <p>
 * Instantiate using {@link ConcourseClientPreferences#open(String)}
 * </p>
 * 
 * @author Jeff Nelson
 */
public class ConcourseClientPreferences extends PreferencesHandler {

    /**
     * Return a {@link ConcourseClientPreferences} wrapper that is backed by the
     * configuration information in {@code file}.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @return the preferences
     */
    public static ConcourseClientPreferences open(String file) {
        try {
            return new ConcourseClientPreferences(file);
        }
        catch (ConfigurationException e) {
            throw Throwables.propagate(e);
        }
    }

    static {
        // Prevent logging from showing up in the console
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ConcourseClientPreferences.class))
                .setLevel(Level.OFF);
    }

    // Defaults
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 1717;
    private static final String DEFAULT_USERNAME = "admin";
    private static final String DEFAULT_PASSWORD = "admin";
    private static final String DEFAULT_ENVIRONMENT = "";

    /**
     * Construct a new instance.
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @throws ConfigurationException
     */
    private ConcourseClientPreferences(String file)
            throws ConfigurationException {
        super(file);
    }

    /**
     * Return the value associated with the {@code environment} key.
     * 
     * @return the environment
     */
    public String getEnvironment() {
        return getString("environment", DEFAULT_ENVIRONMENT);
    }

    /**
     * Return the value associated with the {@code host} key.
     * 
     * @return the host
     */
    public String getHost() {
        return getString("host", DEFAULT_HOST);
    }

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
    public char[] getPassword() {
        return getString("password", DEFAULT_PASSWORD).toCharArray();
    }

    /**
     * Return the value associated with the {@code port} key.
     * 
     * @return the port
     */
    public int getPort() {
        return getInt("port", DEFAULT_PORT);
    }

    /**
     * Return the value associated with the {@code username} key.
     * 
     * @return the username
     */
    public String getUsername() {
        return getString("username", DEFAULT_USERNAME);
    }

    /**
     * Set the value associated with the {@code environment} key.
     * 
     * @param environment
     */
    public void setEnvironment(String environment) {
        setProperty("environment", environment);
    }

    /**
     * Set the value associated with the {@code host} key.
     * 
     * @param host
     */
    public void setHost(String host) {
        setProperty("host", host);
    }

    /**
     * Set the value associated with the {@code password} key.
     * 
     * @param password
     */
    public void setPassword(char[] password) {
        setProperty("password", new String(password));
    }

    /**
     * Set the value associated with the {@code port} key.
     * 
     * @param port
     */
    public void setPort(int port) {
        setProperty("port", port);
    }

    /**
     * Set the value associated with the {@code username} key.
     * 
     * @param username
     */
    public void setUsername(String username) {
        setProperty("username", username);
    }
}