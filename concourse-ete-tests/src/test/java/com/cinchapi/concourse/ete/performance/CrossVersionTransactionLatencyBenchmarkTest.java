/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.ete.performance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;

/**
 * Test how long it takes for a defined set of transactions to run
 *
 * @author Jeff Nelson
 */
public class CrossVersionTransactionLatencyBenchmarkTest
        extends CrossVersionBenchmarkTest {

    static List<Write> writes = new ArrayList<>();

    static {
        int count = 1000;
        int variety = 10;
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < variety; ++i) {
            keys.add(Random.getSimpleString());
        }
        List<Long> records = new ArrayList<>();
        for (long i = 1; i <= variety; ++i) {
            records.add(i);
        }
        java.util.Random rand = new java.util.Random();
        for (int i = 0; i < count; ++i) {
            String key = keys.get(rand.nextInt(variety));
            long record = records.get(rand.nextInt(variety));
            Object value = Random.getObject();
            Write write = new Write(key, value, record);
            writes.add(write);
        }
    }

    @Override
    protected void beforeEachBenchmarkRuns() {}

    @Test
    public void testTransactionLatency() throws InterruptedException {
        int numThreads = 25;
        BlockingQueue<Write> queue = new LinkedBlockingQueue<>(writes);
        CountUpLatch latch = new CountUpLatch();
        AtomicBoolean done = new AtomicBoolean(false);
        List<Thread> threads = new ArrayList<>();
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                for (int i = 0; i < numThreads; ++i) {
                    Thread t = new Thread(() -> {
                        Concourse concourse = server.connect();
                        while (!done.get()) {
                            try {
                                Write write = queue.take();
                                boolean committed = false;
                                while (!committed) {
                                    concourse.stage();
                                    try {
                                        concourse.select(write.key,
                                                write.record);
                                        concourse.add(write.key, write.value,
                                                write.record);
                                        concourse.find(write.key,
                                                Operator.EQUALS, write.value);
                                        if(concourse.commit()) {
                                            committed = true;
                                        }
                                    }
                                    catch (Exception e) {
                                        concourse.abort();
                                    }
                                }
                                latch.countUp();
                            }
                            catch (InterruptedException e) {}
                        }
                        concourse.exit();
                    });
                    threads.add(t);
                    t.start();
                }
                try {
                    latch.await(writes.size());
                    done.set(true);
                    for (Thread thread : threads) {
                        thread.interrupt();
                        thread.join();
                    }
                }
                catch (InterruptedException e) {}
            }
        };

        benchmark.run(3); // warmup
        double elapsed = benchmark.average(3);
        record("time", elapsed);
    }

    private static class Write {

        String key;
        Object value;
        long record;

        Write(String key, Object value, long record) {
            this.key = key;
            this.value = value;
            this.record = record;
        }
    }

}
