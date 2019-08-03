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
import com.cinchapi.concourse.server.cli.core.Options;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.concourse.util.ByteBuffers;

/**
 * A cli for suspending users.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Suspend access for a user")
public class SuspendUserCli extends UserCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public SuspendUserCli(String[] args) {
        super(new Options() {}, args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        try {
            String username;
            if(options.args.isEmpty()) {
                username = console.readLine("User to suspend: ");
            }
            else {
                username = options.args.get(0);
            }
            if(!client.hasUser(ByteBuffers.fromString(username), token)) {
                throw new IllegalArgumentException(
                        "A user named '" + username + "' does not exist");
            }
            else if(options.username.equals(username)) {
                throw new IllegalArgumentException(
                        "The current user cannot suspend itself");
            }
            else {
                client.disableUser(ByteBuffers.fromString(username), token);
                System.out.println("User '" + username + "' is suspended");
            }
        }
        catch (Exception e) {
            die(e.getMessage());
        }

    }

}