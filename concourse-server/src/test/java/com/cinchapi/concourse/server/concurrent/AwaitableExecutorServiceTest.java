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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.time.Time;

/**
 * Unit tests for{@link AwaitableExecutorService}.
 *
 * @author Jeff Nelson
 */
public class AwaitableExecutorServiceTest extends ConcourseBaseTest {

    private AwaitableExecutorService service = null;

    @Override
    public void beforeEachTest() {
        service = new AwaitableExecutorService(Executors.newCachedThreadPool());
    }

    @Test
    public void testServiceDoesntPreventJVMShutdown()
            throws InterruptedException {
        service.await(new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {}

            }

        });
    }

    @Test
    public void testCallingThreadIsBlockedUntilTaskCompletes()
            throws InterruptedException {
        long ts = Time.now();
        service.await(new Runnable() {

            @Override
            public void run() {
                Threads.sleep(100);

            }

        });
        long elapsed = Time.now() - ts;
        Assert.assertTrue(TimeUnit.MILLISECONDS.convert(elapsed,
                TimeUnit.MILLISECONDS) >= 100);
    }

    @Test
    public void testCallingThreadIsBlockedUntilAllTaskCompletes()
            throws InterruptedException {
        long ts = Time.now();
        service.await(new Runnable() {

            @Override
            public void run() {
                Threads.sleep(100);

            }

        }, new Runnable() {

            @Override
            public void run() {
                Threads.sleep(50);

            }

        }, new Runnable() {

            @Override
            public void run() {
                Threads.sleep(150);

            }

        });
        long elapsed = Time.now() - ts;
        Assert.assertTrue(TimeUnit.MILLISECONDS.convert(elapsed,
                TimeUnit.MICROSECONDS) >= 150);
        Assert.assertTrue(TimeUnit.MILLISECONDS.convert(elapsed,
                TimeUnit.MICROSECONDS) <= 300);
    }

    @Test
    public void testThreadIsNotAffectedByBlockingInAnotherThread() {
        final AtomicBoolean aDone = new AtomicBoolean(false);
        final AtomicBoolean aSelfDone = new AtomicBoolean(false);
        final AtomicBoolean bDone = new AtomicBoolean(false);
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    service.await(new Runnable() {

                        @Override
                        public void run() {
                            while (!aDone.get()) {
                                continue;
                            }
                            aSelfDone.set(true);

                        }

                    });
                }
                catch (InterruptedException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }

            }

        });

        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    service.await(new Runnable() {

                        @Override
                        public void run() {
                            Threads.sleep(100);

                        }

                    }, new Runnable() {

                        @Override
                        public void run() {
                            Threads.sleep(50);

                        }

                    }, new Runnable() {

                        @Override
                        public void run() {
                            Threads.sleep(150);

                        }

                    });
                }
                catch (InterruptedException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
                bDone.set(true);

            }

        });
        a.start();
        b.start();
        Threads.sleep(300); // b should take no longer than this
        Assert.assertTrue(bDone.get());
        Assert.assertFalse(aSelfDone.get());
        aDone.set(true);
        Threads.sleep(10);
        Assert.assertTrue(aSelfDone.get());
    }

}
