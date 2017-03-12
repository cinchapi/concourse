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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
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
     * Get the stderr for {@code process}.
     * 
     * @param process
     * @return a collection of error lines
     */
    public static List<String> getStdErr(Process process) {
        return readStream(process.getErrorStream());
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
     * Check if the process with the processId is running.
     * 
     * @param pid Id for the input process.
     * @return true if its running, false if not.
     */
    public static boolean isPidRunning(String pid) {
        Process process = null;
        try {
            if(Platform.isLinux() || Platform.isMacOsX()
                    || Platform.isSolaris()) {
                ProcessBuilder pb = getBuilderWithPipeSupport(
                        "ps aux | grep <pid>");
                process = pb.start();
            }
            else if(Platform.isWindows()) {
                process = Runtime.getRuntime().exec(
                        "TASKLIST /fi \"PID eq " + pid + "\" /fo csv /nh");
            }
            else {
                throw new UnsupportedOperationException(
                        "Cannot check pid on the underlying platform");
            }
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        if(process != null) {
            waitForSuccessfulCompletion(process);
            List<String> lines = readStream(process.getInputStream());
            for (String line : lines) {
                if(line.contains(pid)) {
                    return true;
                }
            }
            return false;
        }
        else {
            return true;
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
     * Return the pid of the current process.
     * 
     * @return pid.
     */
    public static String getCurrentPid() {
        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
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

    private Processes() {} /* no-op */
}
