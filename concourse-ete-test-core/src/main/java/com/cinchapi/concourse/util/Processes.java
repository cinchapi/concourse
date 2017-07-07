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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import com.cinchapi.concourse.annotate.UtilityClass;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Utilities for dealing with {@link Process} objects.
 * 
 * @author jnelson
 */
@UtilityClass
public final class Processes {

    /**
     * Create a {@link ProcessBuilder} that, on the appropriate platforms,
     * sources the standard interactive profile for the user (i.e.
     * ~/.bash_profile).
     * 
     * @param commands a string array containing the program and its arguments
     * @return a {@link ProcessBuilder}
     */
    public static ProcessBuilder getBuilder(String... commands) {
        ProcessBuilder pb = new ProcessBuilder(commands);
        if(!Platform.isWindows()) {
            Map<String, String> env = pb.environment();
            env.put("BASH_ENV", System.getProperty("user.home")
                    + "/.bash_profile");
        }
        return pb;
    }

    /**
     * Create a {@link ProcessBuilder} that, on the appropriate platforms,
     * sources the standard interactive profile for the user (i.e.
     * ~/.bash_profile) and supports the use of the pipe (|) redirection on
     * platforms that allow it.
     * 
     * @param commands a string array containing the program and its arguments
     * @return a {@link ProcessBuilder}
     */
    public static ProcessBuilder getBuilderWithPipeSupport(String... commands) {
        if(!Platform.isWindows()) {
            List<String> listCommands = Lists
                    .newArrayListWithCapacity(commands.length + 2);
            // Need to invoke a shell in which the commands can be run. That
            // shell will properly interpret the pipe(|).
            listCommands.add("/bin/sh");
            listCommands.add("-c");
            for (String command : commands) {
                listCommands.add(command);
            }
            return getBuilder(listCommands.toArray(commands));
        }
        else {
            return getBuilder(commands);
        }
    }

    /**
     * Get the stdout for {@code process}.
     * 
     * @param process
     * @return a collection of output lines
     */
    public static List<String> getStdOut(Process process) {
        return readStream(process.getInputStream());

    }

    /**
     * Read an input stream.
     * 
     * @param stream
     * @return the lines in the stream
     */
    private static List<String> readStream(InputStream stream) {
        try {
            BufferedReader out = new BufferedReader(new InputStreamReader(
                    stream));
            String line;
            List<String> output = Lists.newArrayList();
            while ((line = out.readLine()) != null) {
                output.add(line);
            }
            return output;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Similar to {@link Process#waitFor()} but will throw a
     * {@link RuntimeException} if the process does not have an exit code of
     * {@code 0}.
     * 
     * @param process
     */
    public static void waitForSuccessfulCompletion(Process process) {
        try {
            int exitVal = process.waitFor();
            if(exitVal != 0) {
                throw new RuntimeException(getStdErr(process).toString());
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Get the stderr for {@code process}.
     * 
     * @param process
     * @return a collection of error lines
     */
    public static List<String> getStdErr(Process process) {
        return readStream(process.getErrorStream());
    }

    private Processes() {} /* noinit */

}
