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
package org.cinchapi.concourse.server;

import java.io.File;

import org.apache.thrift.transport.TTransportException;
import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.time.Time;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.google.common.base.Throwables;

/**
 * Benchmarks for {@link ConcourseServer}.
 * 
 * @author jnelson
 */
public class ConcourseServerBenchmark extends AbstractBenchmark {

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 1719;
    private static final String SERVER_DATA_HOME = System
            .getProperty("user.home")
            + File.separator
            + "concourse_"
            + Long.toString(Time.now());
    private static final String SERVER_DATABASE_DIRECTORY = SERVER_DATA_HOME
            + File.separator + "db";
    private static final String SERVER_BUFFER_DIRECTORY = SERVER_DATA_HOME
            + File.separator + "buffer";

    private ConcourseServer server;
    private Concourse client;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            try {
                server = new ConcourseServer(SERVER_PORT,
                        SERVER_BUFFER_DIRECTORY, SERVER_DATABASE_DIRECTORY);
            }
            catch (TTransportException e1) {
                throw Throwables.propagate(e1);
            }
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        server.start();
                    }
                    catch (TTransportException e) {
                        throw Throwables.propagate(e);
                    }

                }

            });
            t.start();
            client = new Concourse.Client(SERVER_HOST, SERVER_PORT, "admin",
                    "admin");
        }

        @Override
        protected void finished(Description description) {
            client.exit();
            server.stop();
            FileSystem.deleteDirectory(SERVER_DATA_HOME);
        }

    };

    // @Test
    // @BenchmarkOptions(benchmarkRounds = 20)
    // public void benchmarkAddLongWrites() {
    // String key = "count";
    // int i = 0;
    // while (i < 1000) {
    // client.add(key, i, i);
    // i++;
    // }
    // }

}
