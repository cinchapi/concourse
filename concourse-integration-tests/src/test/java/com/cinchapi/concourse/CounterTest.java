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
package com.cinchapi.concourse;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.collect.Lists;

/**
 * This test implements a counter that multiple threads try to implement using
 * the verifyAndSwap operation in Concourse and checks to make sure that the
 * counter is implemented correctly (e.g. no missed or duplicate updates)
 * 
 * @author Jeff Nelson
 */
public class CounterTest extends ConcourseIntegrationTest {

    AtomicBoolean running = new AtomicBoolean(true);
    long stoptime = 0;
    List<Integer> counts = Lists.newArrayList();

    @Test
    public void test() throws InterruptedException {
        int size = 10; // more threads give less throughput because of all the
                       // contention
        client.set("count", 0, 1);
        List<Concourse> connections = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            connections.add(Concourse.connect(SERVER_HOST, SERVER_PORT,
                    "admin", "admin"));
        }
        List<Thread> threads = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            threads.add(new Thread(new CounterThread(connections.remove(0))));
        }
        stoptime = System.currentTimeMillis() + 2000;
        for (Thread thread : threads) {
            thread.start();
        }
        while (System.currentTimeMillis() < stoptime) {
            continue;
        }
        running.set(false);
        for (Thread thread : threads) {
            thread.join();
        }
        client.exit();
        int previous = 0;
        for (int count : counts) {
            Assert.assertEquals(count, previous + 1);
            previous = count;
            System.out.println(count);
        }
        client.exit();

    }

    class CounterThread implements Runnable {

        private Concourse client;

        public CounterThread(Concourse client) {
            this.client = client;
        }

        @Override
        public void run() {
            while (running.get()) {
                int count = client.get("count", 1);
                synchronized (counts) {
                    if(client.verifyAndSwap("count", count, 1, count + 1)) {
                        counts.add(count + 1);
                    }
                    else {
                        // just try again...
                    }
                }

            }
            client.exit();

        }
    }

}
