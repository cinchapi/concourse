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
package com.cinchapi.concourse.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;

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
            env.put("BASH_ENV",
                    System.getProperty("user.home") + "/.bash_profile");
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
            BufferedReader out = new BufferedReader(
                    new InputStreamReader(stream));
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

    /**
     * Get current process string representation of process id.
     * 
     * @param process
     * @return String representation of process id
     */
    public static String getCurrentPid(Process process) {
        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }

    /**
     * Check if the process with the processId is running.
     * 
     * @param pid Id for the input process.
     * @return true if its running, false if not.
     */
    public static boolean isProcessRunning(String pid) {
        Process process = null;
        try {
            if(SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC
                    || SystemUtils.IS_OS_MAC_OSX || SystemUtils.IS_OS_UNIX
                    || SystemUtils.IS_OS_SOLARIS || SystemUtils.IS_OS_SUN_OS) {
                ProcessBuilder pb = Processes
                        .getBuilderWithPipeSupport("ps aux | grep <pid>");
                process = pb.start();
                int errCode = process.waitFor();
                if(errCode != 0) {
                    throw new RuntimeException(
                            "Exception while trying to get process status of id : "
                                    + pid);
                }
            }
            else if(SystemUtils.IS_OS_WINDOWS) {
                process = Runtime.getRuntime().exec(
                        "TASKLIST /fi \"PID eq " + pid + "\" /fo csv /nh");
            }
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        if(process != null) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));
            String line = null;
            try {
                while ((line = in.readLine()) != null) {
                    if(line.contains(pid)) {
                        return true;
                    }
                }
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }
        }
        else {
            return false;
        }
        return true; // JavaApp should not shutdown because of exception while
                     // retrieving host process status.
    }

    private Processes() {} /* noinit */

}
