/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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

import com.cinchapi.concourse.server.jmx.ConcourseServerMXBean;

/**
 * A tool that is used to manage the environments in Concourse Server.
 * 
 * @author Jeff Nelson
 */
public class EnvToolCli extends ManagedOperationCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        EnvToolCli cli = new EnvToolCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public EnvToolCli(String[] args) {
        super(new EnvToolOptions(), args);
    }

    @Override
    protected void doTask(ConcourseServerMXBean bean) {
        System.out.println("These are the environments in Concourse Server:");
        System.out.println(bean.listAllEnvironments());
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
