/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.server.cli.core.CommandLineInterfaceInformation;
import com.cinchapi.concourse.server.management.ConcourseManagementService.Client;

/**
 * A CLI for repairing data files.
 *
 * @author Jeff Nelson
 */
@CommandLineInterfaceInformation(description = "Repair the Concourse Server data files")
public class RepairDataCli extends DataCli {

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    public RepairDataCli(String[] args) {
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
            System.out.println(AnyStrings.format(
                    "Looking for corrupted data files in {}. If any are found they will be repaired.",
                    opts.environmentDescription()));
            System.out.println("Follow the logs for more details...");

            client.repairData(opts.environment, token);

            System.out
                    .println(AnyStrings.format("Finished repairing data in {}.",
                            opts.environmentDescription()));
        }
        catch (Exception e) {
            die(e.getMessage());
        }

    }

}
