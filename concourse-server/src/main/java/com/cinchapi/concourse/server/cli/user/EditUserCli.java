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
package com.cinchapi.concourse.server.cli.user;

import java.nio.ByteBuffer;

import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.base.Strings;

/**
 * A CLI for editing users.
 *
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Edit an existing user")
public class EditUserCli extends UserCli {

    public EditUserCli(String[] args) {
        super(new EditUserOptions(), args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        EditUserOptions opts = (EditUserOptions) options;
        try {
            // Verify that the user has specified an edit action
            if(Strings.isNullOrEmpty(opts.userPassword)
                    && Strings.isNullOrEmpty(opts.userRole)) {
                parser.usage();
                throw new IllegalStateException(
                        "Please specify an edit action");
            }
            String username;
            if(opts.args.isEmpty()) {
                username = console.readLine("Which user do you want to edit? ");
            }
            else {
                username = opts.args.get(0);
            }
            ByteBuffer uname = ByteBuffers.fromString(username);

            // Set the user role, if requested
            if(!Strings.isNullOrEmpty(opts.userRole)) {
                try {
                    client.setUserRole(uname, opts.userRole, token);
                    System.out.println("Successfully set the user's role to "
                            + opts.userRole);
                }
                catch (Exception e) {
                    System.err.println(
                            "Unable to set the user's role: " + e.getMessage());
                }
            }

            // Set user password, if requested. NOTE: The password change must
            // be executed last because it'll invalidate the user's current
            // token if the user is changing her own password
            if(!Strings.isNullOrEmpty(opts.userPassword)) {
                try {
                    ByteBuffer pword = ByteBuffers
                            .fromString(opts.userPassword);
                    client.setUserPassword(uname, pword, token);
                    System.out.println("Successfully set the user's password");
                }
                catch (Exception e) {
                    System.err.println("Unable to set the user's password: "
                            + e.getMessage());
                }
            }
        }
        catch (Exception e) {
            die(e.getMessage());
        }
    }

}
