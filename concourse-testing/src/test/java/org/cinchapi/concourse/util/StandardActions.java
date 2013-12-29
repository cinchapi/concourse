/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.server.ConcourseServer;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;

import com.google.common.base.Throwables;

/**
 * This class contains a collection of actions that are common and potentially
 * useful to multiple tests. Every standard action operates on a specified
 * client connection.
 * 
 * @author jnelson
 */
public final class StandardActions {

    /**
     * Import 1000 long values in records 0-999.
     * 
     * @param client
     */
    public static void import1000Longs(Concourse client) {
        System.out.println("Importing 1000 long values...");
        for (int i = 0; i < 1000; i++) {
            client.add("count", i, i);
        }
    }

    /**
     * Import the data from {@code strings.txt}.
     * 
     * @param client
     */
    public static void importWordsDotText(Concourse client) {
        System.out.println("Importing words.txt...");
        try {
            File file = new File(TestData.class.getResource("/words.txt")
                    .getFile());
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            int record = 0;
            while ((line = reader.readLine()) != null) {
                client.add("strings", line, record);
                record++;
            }
            reader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Kill the process that was started from
     * {@link #launchServerInSeparateJVM()}.
     */
    public static void killServerInSeparateJVM() {
        if(SERVER_PROCESS != null) {
            System.out.println("Killing server in separate JVM");
            SERVER_PROCESS.destroy();
            SERVER_PROCESS = null;
            FileSystem.deleteDirectory(SERVER_HOME_DIRECTORY);
            SERVER_HOME_DIRECTORY = null;
        }
    }

    /**
     * Launch a new ConcourseServer process in a separate JVM. This method will
     * use the port defined in conf/concourse.prefs, so it will not start if
     * there is a conflict.
     */
    public static void launchServerInSeparateJVM() {
        String classpath = System.getProperty("java.class.path");
        String java = System.getProperty("java.home") + File.separator + "bin"
                + File.separator + "java";
        String home = "-Duser.dir=" + System.getProperty("user.dir")
                + File.separator + "conf";
        SERVER_HOME_DIRECTORY = System.getProperty("user.home")
                + File.separator + "concourse_" + Time.now();
        try {
            SERVER_PROCESS = new ProcessBuilder(java, "-Xms512m", "-Xmx1024m",
                    "-Declipse=true", home, "-Duser.home="
                            + SERVER_HOME_DIRECTORY, "-cp", classpath,
                    ConcourseServer.class.getName()).start();
            System.out.println("Launched server in separate JVM");
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Wait for the specified {@code duration} of the specified time
     * {@code unit}.
     * 
     * @param duration
     * @param unit
     */
    public static void wait(long duration, TimeUnit unit) {
        try {
            System.out.println("Waiting for " + duration + " " + unit);
            unit.sleep(duration);
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Process SERVER_PROCESS = null;
    private static String SERVER_HOME_DIRECTORY = null;

    private StandardActions() {/* utility class */}

}
