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
package org.cinchapi.concourse;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This test implements a counter that multiple threads try to implement using
 * the verifyAndSwap operation in Concourse and checks to make sure that the
 * counter is implemented correctly (e.g. no missed or duplicate updates)
 * 
 * @author jnelson
 */
public class CounterTest extends ConcourseIntegrationTest {

    AtomicBoolean running = new AtomicBoolean(true);
    long stoptime = 0;
    List<Integer> counts = Collections.synchronizedList(Lists
            .<Integer> newArrayList());

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
                if(client.verifyAndSwap("count", count, 1, count + 1)) {
                    counts.add(count + 1);
                }
                else {
                    // just try again...
                }
            }
            client.exit();

        }
    }

}
