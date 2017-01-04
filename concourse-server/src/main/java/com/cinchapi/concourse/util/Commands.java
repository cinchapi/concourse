/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.io.IOException;
import com.google.common.base.Throwables;

import spark.utils.IOUtils;

/**
 * Abstractions for system commands.
 * 
 * @author Jeff Nelson
 */
public class Commands {

    /**
     * Execute the 'jps' command.
     * 
     * @return the result of executing the command
     */
    public static String jps() {
        return runCommand("jps");
    }

    /**
     * Execute the 'ps aux' command.
     * 
     * @return the result of executing the command
     */
    public static String psAux() {
        return runCommand("ps aux");
    }

    /**
     * Execute an arbitrary {@code command}.
     * 
     * @param command the command to execute
     * @return the output returned from the command
     */
    public static String run(String command) {
        return runCommand(command);
    }

    /**
     * Run a command as a separate process and handle the results (or error)
     * gracefully.
     * 
     * @param command
     * @return the results of the command
     */
    private static String runCommand(String command) {
        try {
            String[] parts = new StringSplitter(command, ' ',
                    SplitOption.TRIM_WHITESPACE).toArray();
            ProcessBuilder pb = new ProcessBuilder(parts);
            Process p = pb.start();
            p.waitFor();
            if(p.exitValue() == 0) {
                return IOUtils.toString(p.getInputStream());
            }
            else {
                throw new RuntimeException(IOUtils.toString(p.getErrorStream()));
            }
        }
        catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

}
