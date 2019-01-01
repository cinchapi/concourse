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
package com.cinchapi.concourse.server.cli.data;

import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;

/**
 * A cli for listing data files.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "List the Concourse Server data files")
public class ListDataCli extends DataCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public ListDataCli(String[] args) {
        super(new DataOptions(), args);
    }

    @Override
    protected boolean requireArgs() {
        return false;
    }

    @Override
    protected void doTask(Client client) {
        DataOptions opts = (DataOptions) options;
        try {
            String list = client.getDumpList(opts.environment, token);
            System.out.println("These are the storage units "
                    + "that are currently dumpable in the '" + opts.environment
                    + "' environment, sorted in reverse chronological "
                    + "order such that units holding newer data appear "
                    + "first. Call this CLI with the `dump` command "
                    + "followed by the id of the storage unit you want "
                    + "to dump.");
            System.out.println(list);
        }
        catch (Exception e) {
            die(e.getMessage());
        }

    }

}
