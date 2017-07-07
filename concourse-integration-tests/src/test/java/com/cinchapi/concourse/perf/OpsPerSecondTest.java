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
package com.cinchapi.concourse.perf;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.base.Stopwatch;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class OpsPerSecondTest extends ConcourseIntegrationTest {

    @Override
    public void beforeEachTest() {
        client.add("table", "foo", 1);
        client.add("id", 1, 1);
        client.add("bar", 1, 1);
    }
    
    @Override
    public void afterEachTest(){
        System.gc();
    }

    @Test
    public void testSelectAll() {
        int count = 20000;
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < count; ++i) {
            client.select("table = foo");
        }
        watch.stop();
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        double seconds = elapsed / 1000.0;
        double ops = count / seconds;
        System.out.println("Select all operations per second: " + ops);
    }

    @Test
    public void testUpdate() {
        int count = 5000;
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < count; ++i) {
            client.set("bar", client.<Integer> get("bar", 1), 1);
        }
        watch.stop();
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        double seconds = elapsed / 1000.0;
        double ops = count / seconds;
        System.out.println("Update operations per second: " + ops);
    }

    @Test
    public void testVerifyAndSwap() {
        int count = 5000;
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < count; ++i) {
            client.verifyAndSwap("bar", i + 1, 1, i + 2);
        }
        watch.stop();
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        double seconds = elapsed / 1000.0;
        double ops = count / seconds;
        System.out.println("Verify and swap operations per second: " + ops);
    }
    
    @Test
    public void testAddDifferentRecords(){
        int count = 5000;
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < count; ++i) {
            client.add("foo", i, i);
        }
        watch.stop();
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        double seconds = elapsed / 1000.0;
        double ops = count / seconds;
        System.out.println("Add to different records operations per second: " + ops);
    }
    
    @Test
    public void testAddSameRecord(){
        int count = 5000;
        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < count; ++i) {
            client.add("foo", i, 1);
        }
        watch.stop();
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        double seconds = elapsed / 1000.0;
        double ops = count / seconds;
        System.out.println("Add to same records operations per second: " + ops);
    }

}
