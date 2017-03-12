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
package com.cinchapi.concourse.server.concurrent;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.concurrent.LockService;
import com.cinchapi.concourse.server.concurrent.Token;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TCollections;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link LockService}.
 * 
 * @author Jeff Nelson
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
    
    @Test
    public void testExclusiveWriteLockForUpgradedToken() throws InterruptedException{
        Token token = Token.wrap(TestData.getLong());
        token.upgrade();
        final WriteLock write2 = lockService.getWriteLock(token);
        write2.lock();
        Thread b = new Thread(new Runnable(){

            @Override
            public void run() {
                Assert.assertFalse(write2.tryLock());
                
            }
            
        });
        b.start();
        b.join();
    }

}
