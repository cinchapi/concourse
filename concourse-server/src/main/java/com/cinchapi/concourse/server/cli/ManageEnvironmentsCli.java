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
import com.cinchapi.concourse.server.management.ConcourseManagementService;

/**
 * A tool that is used to manage the environments in Concourse Server.
 * 
 * @author Jeff Nelson
 */
public class ManageEnvironmentsCli extends ManagementCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        ManageEnvironmentsCli cli = new ManageEnvironmentsCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public ManageEnvironmentsCli(String[] args) {
        super(new EnvToolOptions(), args);
    }

    @Override
    protected void doTask(ConcourseManagementService.Client client) {
        System.out.println("These are the environments in Concourse Server:");
        try {
            System.out.println(client.listAllEnvironments(token));
        }
        catch (TException e) {
            die(e.getMessage());
        }
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author Jeff Nelson
     */
    private static final class EnvToolOptions extends Options {} // NOTE: not
                                                                 // extending
                                                                 // EnvironmentOptions
                                                                 // because any
                                                                 // management
                                                                 // options that
                                                                 // we take on
                                                                 // an
                                                                 // environment
                                                                 // (i.e.
                                                                 // delete)
                                                                 // should be
                                                                 // specified as
                                                                 // --delete
                                                                 // <env>
                                                                 // instead of
                                                                 // --delete -e
                                                                 // <env>

}
