/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.cli.core.ManagementOptions;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.lib.cli.CommandLineInterfaceInformation;

/**
 * A cli for enabling users.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Restore access for a user")
public class EnableUserCli extends UserCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public EnableUserCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask(Client client) {
        try {
            String username;
            if(options.args.isEmpty()) {
                username = console.readLine("User to enable: ");
            }
            else {
                username = options.args.get(0);
            }
            if(!client.hasUser(ByteBuffers.fromUtf8String(username), token)) {
                throw new IllegalArgumentException(
                        "A user named '" + username + "' does not exist");
            }
            else {
                client.enableUser(ByteBuffers.fromUtf8String(username), token);
                System.out.println("User '" + username + "' is enabled");
            }
        }
        catch (Exception e) {
            halt(e.getMessage(), e);
        }

    }

    @Override
    protected ManagementOptions getOptions() {
        return new ManagementOptions();
    }

}