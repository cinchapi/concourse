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

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.base.Stopwatch;

/**
 * Test the speed of find queries
 * 
 * @author Jeff Nelson
 * 
 */
public class FindThroughputTest extends ConcourseIntegrationTest {

    @Test
    public void testFindPerformance1() {
        System.out.println("Doing the testFindPerformance1 test");
        int count = 20000;
        for (int i = 0; i < count; i++) {
            client.add("foo", i, i);
        }
        Stopwatch watch = Stopwatch.createStarted();
        client.find("foo", Operator.GREATER_THAN_OR_EQUALS, 0);
        watch.stop();
        TimeUnit unit = TimeUnit.MILLISECONDS;
        System.out.println(watch.elapsed(unit) + " " + unit);
        System.gc();
    }

    @Test
    public void testFindPerformance2() {
        System.out.println("Doing the testFindPerformance2 test");
        int count = 500;
        for (int i = 0; i < count; i++) {
            for (int j = 0; j <= i; j++) {
                client.add("foo", j, i);
            }
        }
        Stopwatch watch = Stopwatch.createStarted();
        client.find("foo", Operator.BETWEEN, 5, 100);
        watch.stop();
        TimeUnit unit = TimeUnit.MILLISECONDS;
        System.out.println(watch.elapsed(unit) + " " + unit);
        System.gc();

    }

}
