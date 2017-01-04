/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.perf;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.StandardActions;
import com.google.common.base.Stopwatch;

/**
 * 
 * 
 * @author Jeff Nelson
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
