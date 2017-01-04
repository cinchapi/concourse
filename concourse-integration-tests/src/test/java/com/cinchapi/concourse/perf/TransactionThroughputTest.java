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

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * A test of transaction throughput.
 * 
 * @author Jeff Nelson
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
