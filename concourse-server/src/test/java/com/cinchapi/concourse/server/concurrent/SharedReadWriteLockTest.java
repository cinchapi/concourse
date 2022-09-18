/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link SharedReadWriteLock}
 *
 * @author Jeff Nelson
 */
public class SharedReadWriteLockTest extends ConcourseBaseTest {

    private SharedReadWriteLock lock;

    @Override
    public void beforeEachTest() {
        lock = new SharedReadWriteLock();
    }

    @Test
    public void testMultipleConcurrentWriters() throws InterruptedException {
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
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertTrue(success.get());
    }

    @Test
    public void testMultipleConcurrentReaders() throws InterruptedException {
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
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertTrue(success.get());
    }

    @Test
    public void testNoReadersWithMultipleConcurrentWriters()
            throws InterruptedException {
        List<Thread> threads = Lists.newArrayList();
        AtomicBoolean failed = new AtomicBoolean(false);
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
        for (Thread thread : threads) {
            thread.join();
        }
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                if(lock.readLock().tryLock()) {
                    failed.set(true);
                }
            }

        });
        t.start();
        t.join();
        Assert.assertFalse(failed.get());
    }

    @Test
    public void testNoWritersWithMultipleConcurrentReaders()
            throws InterruptedException {
        List<Thread> threads = Lists.newArrayList();
        AtomicBoolean failed = new AtomicBoolean(false);
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
        for (Thread thread : threads) {
            thread.join();
        }
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                if(lock.writeLock().tryLock()) {
                    failed.set(true);
                }
            }

        });
        t.start();
        t.join();
        Assert.assertFalse(failed.get());
    }

    @Test
    public void testNotifyWhenReadNoLongerBlocked()
            throws InterruptedException {
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
        AtomicBoolean succeeded = new AtomicBoolean(false);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                lock.readLock().lock();
                try {
                    succeeded.set(true);
                }
                finally {
                    lock.readLock().unlock();
                }
            }

        });
        t.start();
        Threads.sleep(100);
        unlock.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        t.join();
        Assert.assertTrue(succeeded.get());
    }

    @Test
    public void testNotifyWhenWriteNoLongerBlocked()
            throws InterruptedException {
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
        AtomicBoolean succeeded = new AtomicBoolean(false);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                lock.writeLock().lock();
                try {
                    succeeded.set(true);
                }
                finally {
                    lock.writeLock().unlock();
                }
            }

        });
        t.start();
        Threads.sleep(100);
        unlock.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        t.join();
        Assert.assertTrue(succeeded.get());
    }

}
