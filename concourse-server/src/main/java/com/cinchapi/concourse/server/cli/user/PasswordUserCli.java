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
package com.cinchapi.concourse.server.cli.user;

import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.base.Strings;

/**
 * A cli for changing a users password.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Set a user's password")
public class PasswordUserCli extends UserCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public PasswordUserCli(String[] args) {
        super(new UserOptions(), args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        UserOptions opts = (UserOptions) options;
        try {
            String username;
            if(opts.args.isEmpty()) {
                username = console.readLine("User to edit: ");
            }
            else {
                username = opts.args.get(0);
            }
            if(!client.hasUser(ByteBuffers.fromString(username), token)) {
                console.readLine(
                        "A user named '" + username + "' does not exist. "
                                + "Use CTRL-C to terminate or press RETURN to "
                                + "create this user.");
            }
            if(Strings.isNullOrEmpty(opts.userPassword)) {
                opts.userPassword = console
                        .readLine("New password for " + username + ": ", '*');
                String confirmation = console.readLine("Re-enter password: ",
                        '*');
                if(!opts.userPassword.equals(confirmation)) {
                    throw new SecurityException(
                            "Password confirmation failed so the password for user '"
                                    + username + "' HAS NOT been modifed");
                }
            }
            client.grant(ByteBuffers.fromString(username),
                    ByteBuffers.fromString(opts.userPassword), token);
            System.out.println(
                    "Password for user '" + username + "' has been set");
        }
        catch (Exception e) {
            die(e.getMessage());
        }

    }

}