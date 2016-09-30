/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.server.cli;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import com.google.common.base.Strings;

/**
 * A management tool that gets and sets values from "concourse.prefs" file.
 * 
 * @author Raghav Babu
 */
public class PrefsManagementCli extends ManagedOperationCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        PrefsManagementCli cli = new PrefsManagementCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param opts
     * @param args
     */
    public PrefsManagementCli(String[] args) {
        super(new PrefsManagementOptions(), args);
    }

    @Override
    protected void doTask(ConcourseServerMXBean bean) {
        PrefsManagementOptions opts = ((PrefsManagementOptions) options);
        if(!Strings.isNullOrEmpty(opts.getParam)) {
            String value = bean.getPreference(opts.getParam);
            if(value != null) {
                System.out.println(value);
            }
            else {
                System.out.println(
                        "No value available for the key : " + opts.getParam);
            }
        }
        else if(!Strings.isNullOrEmpty(opts.key)) {
            if(!Strings.isNullOrEmpty(opts.value)) {
                bean.setPreference(opts.key, opts.value);
                System.out.println("Consider it done.");
            }
        }
        else {
            parser.usage();
        }
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author Raghav Babu
     */
    private static class PrefsManagementOptions extends Options {

        @Parameter(names = { "-g",
                "--get" }, description = "Get the value of the specified input key")
        public String getParam;

        @Parameter(names = { "-s",
                "--set" }, description = "Set the value for the specified input key")
        public String key;

        @Parameter(names = { "-v",
                "--value" }, description = "Set this value to the key")
        public String value;

    }

}