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

import com.cinchapi.concourse.server.cli.core.ManagementOptions;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.lib.cli.CommandLineInterfaceInformation;

/**
 * A cli for listing the current user sessions.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "List the current user sessions")
public class SessionsUserCli extends UserCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public SessionsUserCli(String[] args) {
        super(args);
    }

    @Override
    protected void doTask(Client client) {
        try {
            System.out.println("Current User Sessions:");
            System.out.println(client.listAllUserSessions(token));
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
