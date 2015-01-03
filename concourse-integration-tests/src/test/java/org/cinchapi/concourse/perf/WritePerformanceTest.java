/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.util.StandardActions;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.google.common.base.Stopwatch;

/**
 * 
 * 
 * @author jnelson
 */
@RunWith(Theories.class)
public class WritePerformanceTest extends ConcourseIntegrationTest {
    
    public static @DataPoints int[] runs = {0, 1, 2};
    
    @Test
    @Theory
    public void testWriteWordsDotTxt(int run){
        System.out.println("Doing the WritePerformanceTest with words.txt");
        Stopwatch watch = Stopwatch.createStarted();
        StandardActions.importWordsDotText(client);
        watch.stop();
        System.out.println(watch.elapsed(TimeUnit.MILLISECONDS) + " milliseconds");
    }

}
