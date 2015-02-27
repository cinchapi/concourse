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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.util.TCollections;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link LockService}.
 * 
 * @author jnelson
 */
public class LockServiceTest extends ConcourseBaseTest {

    private LockService lockService;

    @Override
    protected void beforeEachTest() {
        lockService = LockService.create();
    }

    @Test
    public void testLockServiceDoesNotEvictLocksThatAreBeingUsedWithHighConcurrencyAndDifferentActions()
            throws InterruptedException {
        int clients = TestData.getScaleCount();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        final Set<String> keys = Sets.newHashSet();
        while (keys.size() < clients) {
            keys.add(TestData.getString());
        }
        final Set<Long> records = Sets.newHashSet();
        while (records.size() < clients) {
            records.add(TestData.getLong());
        }
        Runnable r = new Runnable() {

            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        String key = TCollections.getRandomElement(keys);
                        long record = TCollections.getRandomElement(records);
                        ReadLock readLock = lockService.getReadLock(key, record);
                        readLock.lock();
                        readLock.unlock();
                    }
                    catch (IllegalMonitorStateException e) {
                        e.printStackTrace();
                        done.set(true);
                        failed.set(true);
                    }
                }

            }

        };
        for (int i = 0; i < clients; i++) {
            new Thread(r).start();
        }
        Thread.sleep(TestData.getScaleCount() * 10);
        done.set(true);
        Assert.assertFalse(failed.get());
    }

    @Test
    public void testLockServiceDoesNotEvictLocksThatAreBeingUsedWithHighConcurrency()
            throws InterruptedException {
        int clients = TestData.getScaleCount();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        Runnable r = new Runnable() {

            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        ReadLock readLock = lockService.getReadLock("bar", 1);
                        readLock.lock();
                        readLock.unlock();
                    }
                    catch (IllegalMonitorStateException e) {
                        done.set(true);
                        failed.set(true);
                        e.printStackTrace();
                    }
                }

            }

        };
        for (int i = 0; i < clients; i++) {
            new Thread(r).start();
        }
        Thread.sleep(TestData.getScaleCount() * 10);
        done.set(true);
        Assert.assertFalse(failed.get());
    }

    @Test
    public void testLockServiceDoesNotEvictLocksThatAreBeingUsedEvenWithSomeDelay() {
        final AtomicBoolean wait0 = new AtomicBoolean(true);
        final AtomicBoolean wait1 = new AtomicBoolean(true);
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ReadLock readLock = lockService.getReadLock("foo", 1);
                    readLock.lock();
                    wait0.set(false);
                    while (wait1.get()) {
                        continue;
                    }
                    Thread.sleep(TestData.getScaleCount() * 4);
                    readLock.unlock();
                }
                catch (IllegalMonitorStateException e) {
                    failed.set(true);
                    e.printStackTrace();
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                finally {
                    done.set(true);
                }

            }

        });
        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                while (wait0.get()) {
                    continue;
                }
                ReadLock readLock = lockService.getReadLock("foo", 1);
                readLock.lock();
                readLock.unlock();
                wait1.set(false);

            }

        });
        t1.start();
        t2.start();
        while (!done.get()) {
            continue;
        }
        Assert.assertFalse(failed.get());
    }

    @Test
    public void testLockServiceDoesNotEvictLocksThatAreBeingUsed()
            throws InterruptedException {

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean passed = new AtomicBoolean(true);
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        ReadLock readLock = lockService.getReadLock("foo", 1);
                        readLock.lock();
                        readLock.unlock();
                    }
                    catch (IllegalMonitorStateException e) {
                        e.printStackTrace();
                        passed.set(false);
                        done.set(true);
                        break;
                    }
                }
            }

        });

        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        WriteLock writeLock = lockService.getWriteLock("foo", 1);
                        writeLock.lock();
                        writeLock.unlock();
                    }
                    catch (IllegalMonitorStateException e) {
                        e.printStackTrace();
                        passed.set(false);
                        done.set(true);
                        break;

                    }
                }
            }

        });
        Thread c = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(TestData.getScaleCount() * 10);
                    done.set(true);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
        a.start();
        b.start();
        TestData.getScaleCount(); // make sure that a and b start first
        c.start();
        a.join();
        b.join();
        c.join();
        Assert.assertTrue(passed.get());
    }

}
