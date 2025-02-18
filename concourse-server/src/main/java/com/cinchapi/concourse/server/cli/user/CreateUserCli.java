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

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.cli.core.ManagementOptions;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.lib.cli.CommandLineInterfaceInformation;
import com.google.common.base.Strings;

/**
 * A cli for creating users.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Create a new user")
public class CreateUserCli extends UserCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public CreateUserCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask(Client client) {
        EditUserOptions opts = (EditUserOptions) options;
        try {
            String username;
            if(opts.args.isEmpty()) {
                username = console.readLine("Name for new user: ");
            }
            else {
                username = opts.args.get(0);
            }
            if(client.hasUser(ByteBuffers.fromUtf8String(username), token)) {
                throw new IllegalArgumentException(
                        "A user named '" + username + "' already exists");
            }
            else {
                if(Strings.isNullOrEmpty(opts.userPassword)) {
                    opts.userPassword = console
                            .readLine("Password for " + username + ": ", '*');
                    String confirmation = console
                            .readLine("Re-enter password: ", '*');
                    if(!opts.userPassword.equals(confirmation)) {
                        throw new SecurityException(
                                "Password confirmation failed so '" + username
                                        + "' HAS NOT been created");
                    }
                }
                if(Strings.isNullOrEmpty(opts.userRole)) {
                    opts.userRole = console.readLine(
                            AnyStrings.format("Role for {}: ", username));
                }
                client.createUser(ByteBuffers.fromUtf8String(username),
                        ByteBuffers.fromUtf8String(opts.userPassword),
                        opts.userRole, token);
                System.out.println(
                        "New user '" + username + "' has been created");
            }
        }
        catch (Exception e) {
            halt(e.getMessage(), e);
        }

    }

    @Override
    protected ManagementOptions getOptions() {
        return new EditUserOptions();
    }

}
