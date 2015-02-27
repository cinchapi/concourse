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
package org.cinchapi.concourse.server.concurrent;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link ReardWriteSharedLock} objects
 * 
 * @author jnelson
 */
public class ReadWriteSharedLockTest extends ConcourseBaseTest {

    private ReadWriteSharedLock lock;

    @Override
    public void beforeEachTest() {
        lock = new ReadWriteSharedLock();
    }

    @Test
    public void testMultipleConcurrentWriters() {
        List<Thread> threads = Lists.newArrayList();
        final AtomicBoolean success = new AtomicBoolean(true);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                if(!lock.writeLock().tryLock()) {
                    success.set(false);
                }
            }

        };
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            threads.add(new Thread(runnable));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        Assert.assertTrue(success.get());
    }

    @Test
    public void testMultipleConcurrentReaders() {
        List<Thread> threads = Lists.newArrayList();
        final AtomicBoolean success = new AtomicBoolean(true);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                if(!lock.readLock().tryLock()) {
                    success.set(false);
                }
            }

        };
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            threads.add(new Thread(runnable));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        Assert.assertTrue(success.get());
    }

    @Test
    public void testNoReadersWithMultipleConcurrentWriters() {
        List<Thread> threads = Lists.newArrayList();
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                lock.writeLock().lock();
            }

        };
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            threads.add(new Thread(runnable));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        new Thread(new Runnable() {

            @Override
            public void run() {
                Assert.assertFalse(lock.readLock().tryLock());
            }

        }).start();
    }

    @Test
    public void testNoWritersWithMultipleConcurrentReaders() {
        List<Thread> threads = Lists.newArrayList();
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                lock.readLock().lock();
            }

        };
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            threads.add(new Thread(runnable));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        new Thread(new Runnable() {

            @Override
            public void run() {
                Assert.assertFalse(lock.writeLock().tryLock());
            }

        }).start();
    }

    @Test
    public void testNotifyWhenReadNoLongerBlocked() {
        List<Thread> threads = Lists.newArrayList();
        final AtomicBoolean unlock = new AtomicBoolean(false);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                lock.writeLock().lock();
                while (!unlock.get()) {
                    continue;
                }
                lock.writeLock().unlock();
            }

        };

        for (int i = 0; i < TestData.getScaleCount(); i++) {
            threads.add(new Thread(runnable));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        new Thread(new Runnable() {

            @Override
            public void run() {
                long t1 = Time.now();
                lock.readLock().lock();
                try {
                    long elapsed = TimeUnit.MILLISECONDS.convert(Time.now()
                            - t1, TimeUnit.MICROSECONDS);
                    Assert.assertTrue(elapsed >= (.80 * 100)); // sleep time is
                                                               // imprecise, so
                                                               // accept 80%
                                                               // accuracy
                }
                finally {
                    lock.readLock().unlock();
                }

            }

        }).start();
        Threads.sleep(100);
        unlock.set(true);
    }

    @Test
    public void testNotifyWhenWriteNoLongerBlocked() {
        List<Thread> threads = Lists.newArrayList();
        final AtomicBoolean unlock = new AtomicBoolean(false);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                lock.readLock().lock();
                while (!unlock.get()) {
                    continue;
                }
                lock.readLock().unlock();
            }

        };

        for (int i = 0; i < TestData.getScaleCount(); i++) {
            threads.add(new Thread(runnable));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        new Thread(new Runnable() {

            @Override
            public void run() {
                long t1 = Time.now();
                lock.writeLock().lock();
                try {
                    long elapsed = TimeUnit.MILLISECONDS.convert(Time.now()
                            - t1, TimeUnit.MICROSECONDS);
                    Assert.assertTrue(elapsed >= (.80 * 100)); // sleep time is
                                                               // imprecise, so
                                                               // accept 80%
                                                               // accuracy
                }
                finally {
                    lock.writeLock().unlock();
                }

            }

        }).start();
        Threads.sleep(100);
        unlock.set(true);
    }

    @Test
    public void testGetWriteLockCount() {
        int count = TestData.getScaleCount();
        final AtomicInteger finished = new AtomicInteger(0);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                lock.writeLock().lock();
                finished.incrementAndGet();
            }

        };
        for (int i = 0; i < count; i++) {
            new Thread(runnable).start();
        }
        while (finished.get() < count) {
            continue;
        }
        Assert.assertEquals(lock.getWriteLockCount(), count);
    }

    @Test
    public void testGetWriteHoldCount() {
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            lock.writeLock().lock();
        }
        Assert.assertEquals(lock.getWriteHoldCount(), count);
    }

    @Test
    public void testGetReadLockCount() {
        int count = TestData.getScaleCount();
        final AtomicInteger finished = new AtomicInteger(0);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                lock.readLock().lock();
                finished.incrementAndGet();
            }

        };
        for (int i = 0; i < count; i++) {
            new Thread(runnable).start();
        }
        while (finished.get() < count) {
            continue;
        }
        Assert.assertEquals(lock.getReadLockCount(), count);
    }

    @Test
    public void testGetReadHoldCount() {
        int count = TestData.getScaleCount();
        for (int i = 0; i < count; i++) {
            lock.readLock().lock();
        }
        Assert.assertEquals(lock.getReadHoldCount(), count);
    }

    @Test
    public void testIsWriteLockedByCurrentThread() {
        Runnable a = new Runnable() {

            @Override
            public void run() {
                Assert.assertFalse(lock.isWriteLockedByCurrentThread());
            }

        };
        Runnable b = new Runnable() {

            @Override
            public void run() {
                lock.writeLock().lock();
                Assert.assertTrue(lock.isWriteLockedByCurrentThread());
            }

        };
        for(int i = 0; i < TestData.getScaleCount(); i++){
            Thread t = i %3 == 0 ? new Thread(a) : new Thread(b);
            t.start();
        }
    }

}
