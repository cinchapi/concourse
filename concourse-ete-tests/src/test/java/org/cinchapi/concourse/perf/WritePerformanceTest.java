/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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

import org.cinchapi.concourse.util.StandardActions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

/**
 * A test that measures the performance of various write activities.
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public class WritePerformanceTest extends ConcoursePerformanceTest {

    @Rule
    public TestName name = new TestName();

    @DataPoints
    public static Integer[] TEST_RUNS = { 1, 2, 3, 4, 5 };

    @Test
    @Theory
    public void testInsertWordsDotText(Integer testRun) {
        watch.start();
        StandardActions.importWordsDotText(client);
        watch.stop();
        StandardActions.killServerInSeparateJVM();
        System.out.println("*** " + name.getMethodName() + ": Test Run "
                + testRun + " took " + watch.elapsed(TimeUnit.MILLISECONDS)
                + " ms");
    }

    @Test
    @Theory
    public void testInsert1000Longs(Integer testRun) {
        watch.start();
        StandardActions.import1000Longs(client);
        watch.stop();
        StandardActions.killServerInSeparateJVM();
        System.out.println("*** " + name.getMethodName() + ": Test Run "
                + testRun + " took " + watch.elapsed(TimeUnit.MILLISECONDS)
                + " ms");
    }

}
