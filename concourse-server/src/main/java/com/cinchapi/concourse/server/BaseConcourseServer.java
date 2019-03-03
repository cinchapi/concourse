/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import com.cinchapi.concourse.security.Permission;
import com.cinchapi.concourse.security.Role;
import com.cinchapi.concourse.security.UserService;
import com.cinchapi.concourse.server.aop.ThrowsManagementExceptions;
import com.cinchapi.concourse.server.aop.VerifyAccessToken;
import com.cinchapi.concourse.server.aop.VerifyAdminRole;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.management.ClientInvokable;
import com.cinchapi.concourse.server.management.ConcourseManagementService;
import com.cinchapi.concourse.server.plugin.PluginManager;
import com.cinchapi.concourse.server.plugin.PluginRestricted;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.thrift.AccessToken;
import com.cinchapi.concourse.util.Environments;
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

    /*
     * IMPORTANT NOTICE
     * ----------------
     * DO NOT declare as FINAL any methods that are intercepted by Guice because
     * doing so will cause the interception to silently fail. See
     * https://github.com/google/guice/wiki/AOP#limitations for more details.
     */

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    @ClientInvokable
    public void createUser(ByteBuffer username, ByteBuffer password,
            String role, AccessToken creds) throws TException {
        users().create(username, password, Role.valueOfIgnoreCase(role));

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void deleteUser(ByteBuffer username, AccessToken creds)
            throws TException {
        users().delete(username);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void disableUser(ByteBuffer username, AccessToken creds)
            throws TException {
        users().disable(username);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public String dump(String id, String environment, AccessToken creds)
            throws TException {
        return getEngine(environment).dump(id);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void enableUser(ByteBuffer username, AccessToken creds)
            throws TException {
        users().enable(username);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public String getDumpList(String environment, AccessToken creds)
            throws TException {
        return getEngine(environment).getDumpList();
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    @ClientInvokable
    public void grant(ByteBuffer username, String permission,
            String environment, AccessToken creds) throws TException {
        users().grant(username, Permission.valueOfIgnoreCase(permission),
                Environments.sanitize(environment));

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public boolean hasUser(ByteBuffer username, AccessToken creds)
            throws TException {
        return users().exists(username);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void installPluginBundle(String file, AccessToken creds)
            throws TException {
        plugins().installBundle(file);
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public String listAllEnvironments(AccessToken creds) throws TException {
        return TCollections.toOrderedListString(
                TSets.intersection(FileSystem.getSubDirs(getBufferStore()),
                        FileSystem.getSubDirs(getDbStore())));
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public String listAllUserSessions(AccessToken creds) throws TException {
        return TCollections
                .toOrderedListString(users().tokens.describeActiveSessions());
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public String listPluginBundles(AccessToken creds) throws TException {
        return TCollections.toOrderedListString(plugins().listBundles());
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void revoke(ByteBuffer username, String environment,
            AccessToken creds) throws TException {
        users().revoke(username, Environments.sanitize(environment));
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public Map<Long, Map<String, String>> runningPluginsInfo(AccessToken creds)
            throws TException {
        return plugins().runningPlugins();
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void setUserPassword(ByteBuffer username, ByteBuffer password,
            AccessToken creds) throws TException {
        users().setPassword(username, password);

    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void setUserRole(ByteBuffer username, String role, AccessToken creds)
            throws TException {
        users().setRole(username, Role.valueOfIgnoreCase(role));
    }

    @Override
    @PluginRestricted
    @ThrowsManagementExceptions
    @VerifyAccessToken
    @VerifyAdminRole
    public void uninstallPluginBundle(String name, AccessToken creds)
            throws TException {
        plugins().uninstallBundle(name);
    }

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
    protected abstract PluginManager plugins();

    /**
     * Return the {@link UserService} used by the server.
     * 
     * @return the {@link UserService}
     */
    protected abstract UserService users();

}
