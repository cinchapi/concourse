/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.cli;

import org.apache.thrift.TException;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.server.management.ConcourseManagementService;

/**
 * A debugging tool that dumps the contents of a specified {@link Block}.
 * 
 * @author Jeff Nelson
 */
public final class ManageDataCli extends ManagementCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        ManageDataCli cli = new ManageDataCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     *
     * @param args
     */
    public ManageDataCli(String[] args) {
        super(new DumpToolOptions(), args);
    }

    @Override
    protected void doTask(ConcourseManagementService.Client client) {
        DumpToolOptions opts = ((DumpToolOptions) options);
        if(((DumpToolOptions) options).id != null) {
            try {
                System.out.println(client.dump(opts.id, opts.environment,
                            token));
            }
            catch (TException e) {
                die(e.getMessage());
            }
        }
        else {
            System.out.println("These are the storage units "
                    + "that are currently dumpable in the '" + opts.environment
                    + "' environment, sorted in reverse chronological "
                    + "order such that units holding newer data appear "
                    + "first. Call this CLI with the `-i or --id` flag "
                    + "followed by the id of the storage unit you want "
                    + "to dump.");
            try {
                System.out.println(client.getDumpList(opts.environment,token));
            }
            catch (TException e) {
                die(e.getMessage());
            }
        }

    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author Jeff Nelson
     */
    private static class DumpToolOptions extends EnvironmentOptions {

        @Parameter(names = { "-i", "--id" }, description = "The id of the storage component to dump. Specify an ID of 'BUFFER' to dump the Buffer content")
        public String id;

        @Parameter(names = { "-l", "--list" }, description = "[DEPRECATED] List the ids of the blocks that can be dumped")
        public boolean list;

    }

}
