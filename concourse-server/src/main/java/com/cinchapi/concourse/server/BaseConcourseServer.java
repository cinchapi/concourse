/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.thrift.TException;

import com.cinchapi.concourse.security.UserService;
import com.cinchapi.concourse.security.UserService.Role;
import com.cinchapi.concourse.server.aop.ThrowsManagementExceptions;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.management.ConcourseManagementService;
import com.cinchapi.concourse.server.plugin.PluginManager;
import com.cinchapi.concourse.server.plugin.PluginRestricted;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.thrift.SecurityException;
import com.cinchapi.concourse.util.TCollections;
import com.cinchapi.concourse.util.TSets;

/**
 * Base implementation of Concourse Server.
 * <p>
 * In general, this class provides implementations for non-client API methods
 * (i.e. the management API).
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class BaseConcourseServer
        implements ConcourseManagementService.Iface {

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final void createUser(ByteBuffer username, ByteBuffer password,
            String role, AccessToken creds) throws TException {
        checkAccess(creds);
        Role userRole = Role.valueOfIgnoreCase(role);
        userService().create(username, password, userRole);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final void deleteUser(ByteBuffer username, AccessToken creds)
            throws TException {
        checkAccess(creds);
        userService().delete(username);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final void disableUser(ByteBuffer username, AccessToken creds)
            throws TException {
        checkAccess(creds);
        userService().disable(username);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final String dump(String id, String environment, AccessToken creds)
            throws TException {
        checkAccess(creds);
        return getEngine(environment).dump(id);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final void enableUser(ByteBuffer username, AccessToken creds)
            throws TException {
        checkAccess(creds);
        userService().enable(username);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final String getDumpList(String environment, AccessToken creds)
            throws TException {
        checkAccess(creds);
        return getEngine(environment).getDumpList();
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final boolean hasUser(ByteBuffer username, AccessToken creds)
            throws TException {
        checkAccess(creds);
        return userService().exists(username);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final void installPluginBundle(String file, AccessToken creds)
            throws TException {
        checkAccess(creds);
        getPluginManager().installBundle(file);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final String listAllEnvironments(AccessToken creds)
            throws TException {
        checkAccess(creds);
        return TCollections.toOrderedListString(
                TSets.intersection(FileSystem.getSubDirs(getBufferStore()),
                        FileSystem.getSubDirs(getDbStore())));
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final String listAllUserSessions(AccessToken creds)
            throws TException {
        checkAccess(creds);
        return TCollections.toOrderedListString(
                userService().tokens.describeActiveSessions());
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final String listPluginBundles(AccessToken creds) throws TException {
        checkAccess(creds);
        return TCollections
                .toOrderedListString(getPluginManager().listBundles());
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public Map<Long, Map<String, String>> runningPluginsInfo(AccessToken creds)
            throws TException {
        return getPluginManager().runningPlugins();
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public void setUserPassword(ByteBuffer username, ByteBuffer password,
            AccessToken creds) throws TException {
        checkAccess(creds);
        UserService users = userService();
        users.setPassword(username, password);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public void setUserRole(ByteBuffer username, String role, AccessToken creds)
            throws TException {
        checkAccess(creds);
        UserService users = userService();
        users.setRole(username, Role.valueOfIgnoreCase(role));

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    public final void uninstallPluginBundle(String name, AccessToken creds)
            throws TException {
        checkAccess(creds);
        getPluginManager().uninstallBundle(name);
    }

    /**
     * Check to make sure that {@code creds} and {@code transaction} are valid
     * and are associated with one another.
     *
     * @param creds
     * @throws SecurityException
     * @throws IllegalArgumentException
     */
    protected abstract void checkAccess(AccessToken creds) throws TException;

    /**
     * Return the {@link UserService} used by the server.
     * 
     * @return the {@link UserService}
     */
    protected abstract UserService userService();

    /**
     * Return the location where the server store's contents in the buffer.
     * 
     * @return the bufferStore
     */
    protected abstract String getBufferStore();

    /**
     * Return the location where the server store's contents in the database.
     * 
     * @return the dbStore
     */
    protected abstract String getDbStore();

    /**
     * Return the {@link Engine} that corresponds to the {@code environment}.
     * 
     * @param environment the name of the environment
     * @return the environment's {@link Engine}
     */
    protected abstract Engine getEngine(String environment);

    /**
     * Return the {@link PluginManager} used by the server.
     * 
     * @return the {@link PluginManager}
     */
    protected abstract PluginManager getPluginManager();

}
