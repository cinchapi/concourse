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
package org.cinchapi.concourse.server.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.time.Time;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link BlockingExecutorService}.
 * 
 * @author jnelson
 */
public class BlockingExecutorServiceTest extends ConcourseBaseTest {

    private BlockingExecutorService service = null;

    @Override
    public void beforeEachTest() {
        service = BlockingExecutorService.create();
    }

    @Test
    public void testServiceDoesntPreventJVMShutdown() {
        service.execute(new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {}

            }

        });
    }

    @Test
    public void testCallingThreadIsBlockedUntilTaskCompletes() {
        long ts = Time.now();
        service.execute(new Runnable() {

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
    public void testCallingThreadIsBlockedUntilAllTaskCompletes() {
        long ts = Time.now();
        service.execute(new Runnable() {

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
                service.execute(new Runnable() {

                    @Override
                    public void run() {
                        while (!aDone.get()) {
                            continue;
                        }
                        aSelfDone.set(true);

                    }

                });

            }

        });

        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                service.execute(new Runnable() {

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
