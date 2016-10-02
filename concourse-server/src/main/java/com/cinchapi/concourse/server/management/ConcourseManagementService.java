package com.cinchapi.concourse.server.management;

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.jmx.ManagedOperation;

public interface ConcourseManagementService {

    /**
     * The url used for connecting to the server's JMX service.
     */
    public static final String JMX_SERVICE_URL = "service:jmx:rmi:///jndi/rmi://localhost:"
            + GlobalState.JMX_PORT + "/jmxrmi";

    /**
     * Disable the user(i.e. the user cannot be authenticated for any purposes,
     * even with the correct password).
     *
     * @param username
     */
    public void disableUser(byte[] username);

    /**
     * Return a string that contains the dumps for all the storage units (i.e.
     * buffer, primary, secondary, search) in {@code environment} identified by
     * {@code id}.
     *
     * @param id
     * @param environment
     * @return the dump string
     */
    @ManagedOperation
    public String dump(String id, String environment);

    /**
     * Enable the user(i.e. the user can be authenticated with the correct
     * password).
     *
     * @param username
     */
    public void enableUser(byte[] username);


    /**
     * Return a string that contains a list of the ids in the
     * {@code environment} for all the blocks that can be dumped using
     * {@link #dump(String, String)}.
     *
     * @param environment
     * @return the dump list
     */
    @ManagedOperation
    public String getDumpList(String environment);

    /**
     * Return the release version of the server.
     *
     * @return the server Version string
     */
    @ManagedOperation
    public String getServerVersion();

    /**
     * Grant access to the user identified by the combination of
     * {@code username} and {@code password}.
     *
     * @param username
     * @param password
     */
    public void grant(byte[] username, byte[] password);

    /**
     * Return {@code true} if the server can be accessed
     * by a user identified by {@code username}.
     *
     * @param username
     * @return true/false
     */
    @ManagedOperation
    public boolean hasUser(byte[] username);

    /**
     * Install the plugin bundle contained in the {@code file}.
     *
     * @param file
     */
    public void installPluginBundle(String file);

    /**
     * Return the names of all the environments that exist within Concourse
     * Server. An environment is said to exist if at least one user has
     * established a connection to that environment.
     *
     * @return a string containing all of the environments
     */
    @ManagedOperation
    public String listAllEnvironments();

    /**
     * Return a description of all the currently active user sessions within
     * Concourse Server.
     *
     * @return a string containing all the user sessions
     */
    @ManagedOperation
    public String listAllUserSessions();

    /**
     * List all of the plugins that are available.
     *
     * @return a String containing a list of all the available plugins
     */
    public String listPluginBundles();

    /**
     * Return {@code true} if {@code username} and {@code password} is a valid
     * combination to login to the server for the purpose of performing a
     * managed operation. This method should only be used to authenticate a user
     * for the purpose of performing a single operation.
     *
     * @param username
     * @param password
     * @return {@code true} if the credentials are valid
     */
    @ManagedOperation
    public boolean managementLogin(byte[] username, byte[] password);

    /**
     * Remove the user identified by {@code username}.
     *
     * @param username
     */
    public void revoke(byte[] username);

    /**
     * Uninstall the plugin bundled referred to as {@code name}.
     *
     * @param name the name of the plugin bundle
     */
    public void uninstallPluginBundle(String name);
}
