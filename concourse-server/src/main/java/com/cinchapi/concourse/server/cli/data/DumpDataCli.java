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
package com.cinchapi.concourse.server.cli.data;

import com.cinchapi.concourse.server.cli.core.EnvironmentManagementOptions;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;
import com.cinchapi.lib.cli.CommandLineInterfaceInformation;

/**
 * A cli for dumping data files.
 * 
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Dump the contents of a Concourse data file")
class DumpDataCli extends DataCli {

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public DumpDataCli(String[] args) {
        super(args);
    }

    @Override
    protected boolean requireArgs() {
        return true;
    }

    @Override
    protected void doTask(Client client) {
        EnvironmentManagementOptions opts = options();
        String id = options.args.get(0);
        try {
            System.out.println(client.dump(id, opts.environment, token));
        }
        catch (Exception e) {
            halt(e.getMessage(), e);
        }
    }

}
