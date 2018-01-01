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
package com.cinchapi.concourse.server.cli.env;

import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;

/**
 * A cli for listing environments.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "List the Concourse Server environments")
public class ListEnvironmentCli extends EnvironmentCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public ListEnvironmentCli(String[] args) {
        super(new EnvironmentOptions(), args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        System.out.println("These are the environments in Concourse Server:");
        try {
            System.out.println(client.listAllEnvironments(token));
        }
        catch (Exception e) {
            die(e.getMessage());
        }

    }

}
