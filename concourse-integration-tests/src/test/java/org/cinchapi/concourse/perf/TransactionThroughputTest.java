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

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.TransactionException;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * A test of transaction throughput.
 * 
 * @author jnelson
 */
public class TransactionThroughputTest extends ConcourseIntegrationTest {

    AtomicBoolean running = new AtomicBoolean(true);
    long stoptime = 0;
    AtomicLong rounds = new AtomicLong(0);
    AtomicLong failed = new AtomicLong(0);

    @Test
    public void test() {
        try {
            System.out.println("Doing the TransactionThroughputTest");
            int size = 10;
            // Pre-seed the data
            for (int i = 0; i < size; i++) {
                client.clear(i);
                client.set("username", Integer.toString(i), i);
                client.set("password", Integer.toString(i), i);
                client.set("age", Integer.toString(i), i);
            }

            List<Concourse> connections = Lists.newArrayList();
            for (int i = 0; i < size; i++) {
                connections.add(Concourse.connect(SERVER_HOST, SERVER_PORT,
                        "admin", "admin"));
            }
            List<Thread> threads = Lists.newArrayList();
            for (int i = 0; i < size; i++) {
                threads.add(new Thread(new ConcourseRunnable(connections
                        .remove(0))));
            }
            stoptime = System.currentTimeMillis() + 10000;
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
            System.out.println("rounds=" + rounds.get());
            System.out.println("failed=" + failed.get());
            client.exit();

        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    class ConcourseRunnable implements Runnable {

        private Concourse client;
        private Random random = new Random();

        public ConcourseRunnable(Concourse client) {
            this.client = client;
        }

        @Override
        public void run() {
            while (running.get()) {
                int id = random.nextInt(10);
                try {
                    client.stage();
                    client.set("username",
                            ((String) client.get("username", id)) + "n", id);
                    client.set("password",
                            ((String) client.get("password", id)) + "n", id);
                    client.set("age", ((String) client.get("age", id)) + "n",
                            id);
                    if(client.commit()) {
                        rounds.incrementAndGet();
                    }
                    else {
                        failed.incrementAndGet();
                    }
                }
                catch (TransactionException e) {
                    client.abort();
                    failed.incrementAndGet();
                }

            }
            client.exit();
        }
    }

}
