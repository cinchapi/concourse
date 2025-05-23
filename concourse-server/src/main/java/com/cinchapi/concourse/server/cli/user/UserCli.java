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
package com.cinchapi.concourse.server.cli.user;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.server.cli.core.ManagementCommandLineInterface;
import com.cinchapi.concourse.server.cli.core.ManagementOptions;

/**
 * Entry point for management CLIs to add/modify/remove user access to the
 * server.
 * 
 * @author Jeff Nelson
 */
public abstract class UserCli extends ManagementCommandLineInterface {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public UserCli(String[] args) {
        super(args);
    }

    /**
     * Special options for managing a users password.
     * 
     * @author Jeff Nelson
     */
    protected static class UserPasswordOptions extends ManagementOptions {

        @Parameter(names = {
                "--set-password" }, description = "Password of the user that is being managed (e.g. the non-invoking user)")
        public String userPassword;

    }

    /**
     * Special options for editing a user.
     * 
     * @author Jeff Nelson
     */
    protected static class EditUserOptions extends UserPasswordOptions {

        @Parameter(names = {
                "--set-role" }, description = "The role to set for the user that is being managed (e.g. the non-invoking user)")
        public String userRole;
    }

    /**
     * Special options for managing permissions.
     *
     * @author Jeff Nelson
     */
    protected static class PermissionOptions extends ManagementOptions {

        @Parameter(names = { "-e",
                "--environment" }, description = "The environment in which the permission is granted (if not specified, the default environment is used)")
        public String environment = "";
    }

    /**
     * Special options for managing permissions.
     *
     * @author Jeff Nelson
     */
    protected static class GrantPermissionOptions extends PermissionOptions {

        @Parameter(names = {
                "--permission", }, description = "The permission to grant", required = true)
        public String permission;
    }
}
