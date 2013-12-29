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
package org.cinchapi.concourse.perf;

import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.util.StandardActions;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Stopwatch;

/**
 * The base class for ALL Concourse performance tests. A performance test should
 * extend this class to ensure that it measures realistic client/server
 * interaction between two different JVMs.
 * 
 * @author jnelson
 */
public abstract class ConcoursePerformanceTest {

    /**
     * The client that is used to interact with the server.
     */
    protected Concourse client;

    /**
     * The subclass should start and stop this watch to measuring performance.
     */
    protected Stopwatch watch;

    @Rule
    public TestWatcher watcher = new TestWatcher() {

        @Override
        protected void finished(Description description) {
            client.exit();
            StandardActions.killServerInSeparateJVM();
        }

        @Override
        protected void starting(Description description) {
            StandardActions.launchServerInSeparateJVM();
            StandardActions.wait(1, TimeUnit.SECONDS); // wait for server
                                                       // to be ready to
                                                       // accept client
                                                       // connection
            client = Concourse.connect();
            watch = Stopwatch.createUnstarted();
        }

    };

}
