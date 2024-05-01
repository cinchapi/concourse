/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.server.concurrent.LockBroker.Permit;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TCollections;
import com.cinchapi.concourse.util.TestData;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link LockBroker}.
 *
 * @author Jeff Nelson
 */
public class LockBrokerTest extends ConcourseBaseTest {

    private LockBroker broker;

    @Override
    protected void beforeEachTest() {
        broker = LockBroker.create();
    }

    @Test
    public void testLockBrokerDoesNotEvictLocksThatAreBeingUsedWithHighConcurrencyAndDifferentActions()
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
                        Permit permit = broker
                                .readLock(Token.wrap(key, record));
                        permit.release();
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
    public void testSharedWriteLockForUpgradedToken()
            throws InterruptedException {
        Token token = Token.shareable(TestData.getLong());
        broker.writeLock(token);
        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                // Multiple writers
                Assert.assertNotNull(broker.tryWriteLock(token));
                // Block readers
                Assert.assertNull(broker.tryReadLock(token));

            }

        });
        b.start();
        b.join();
    }

    @Test
    public void testSharedReadLockForUpgradedToken()
            throws InterruptedException {
        Token token = Token.shareable(TestData.getLong());
        broker.readLock(token);
        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                // Block writers
                Assert.assertNull(broker.tryWriteLock(token));
                // Multiple readers
                Assert.assertNotNull(broker.tryReadLock(token));

            }

        });
        b.start();
        b.join();
    }

    @Test
    public void testLockBrokerDoesNotEvictLocksThatAreBeingUsed()
            throws InterruptedException {

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean passed = new AtomicBoolean(true);
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        Permit permit = broker.readLock(Token.wrap("foo", 1));
                        permit.release();
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
                        Permit permit = broker.writeLock(Token.wrap("foo", 1));
                        permit.release();
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
    public void testLockBrokerDoesNotEvictLocksThatAreBeingUsedWithHighConcurrency()
            throws InterruptedException {
        int clients = TestData.getScaleCount();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        Runnable r = new Runnable() {

            @Override
            public void run() {
                while (!done.get()) {
                    try {
                        Permit permit = broker.readLock(Token.wrap("bar", 1));
                        permit.release();
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
    public void testLockBrokerDoesNotEvictLocksThatAreBeingUsedEvenWithSomeDelay() {
        final AtomicBoolean wait0 = new AtomicBoolean(true);
        final AtomicBoolean wait1 = new AtomicBoolean(true);
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Permit permit = broker.readLock(Token.wrap("foo", 1));
                    wait0.set(false);
                    while (wait1.get()) {
                        continue;
                    }
                    Thread.sleep(TestData.getScaleCount() * 4);
                    permit.release();
                }
                catch (IllegalMonitorStateException e) {
                    failed.set(true);
                    e.printStackTrace();
                }
                catch (InterruptedException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
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
                Permit permit = broker.readLock(Token.wrap("foo", 1));
                permit.release();
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
    public void testWriteIsRangeBlockedIfReadingAllValues() {
        RangeToken read = RangeToken.forReading(Text.wrapCached("foo"),
                Operator.BETWEEN, Value.NEGATIVE_INFINITY,
                Value.POSITIVE_INFINITY);
        Permit permit = broker.readLock(read);
        Assert.assertNull(broker.tryWriteLock(RangeToken
                .forWriting(Text.wrapCached("foo"), TestData.getValue())));
        permit.release();
        Assert.assertNotNull(broker.tryWriteLock(RangeToken
                .forWriting(Text.wrapCached("foo"), TestData.getValue())));
    }

    @Test
    public void testWriteLtLowerValueIsNotRangeBlockedIfReadingBw()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value1 = Variables.register("value1", TestData.getValue());
        final Value value2 = Variables.register("value2", increase(value1));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeToken token = RangeToken.forReading(key, Operator.BETWEEN,
                        value1, value2);
                Permit permit = broker.readLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Value value3 = Variables.register("value3", decrease(value1));
        Assert.assertNotNull(
                broker.tryWriteLock(RangeToken.forWriting(key, value3)));
        finishLatch.countDown();
    }

    @Test
    public void testWriteGtHigherValueIsNotRangeBlockedIfReadingBw()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value1 = Variables.register("value1", TestData.getValue());
        final Value value2 = Variables.register("value2", increase(value1));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeToken token = RangeToken.forReading(key, Operator.BETWEEN,
                        value1, value2);
                Permit permit = broker.readLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Value value3 = Variables.register("value3", increase(value2));
        Assert.assertNotNull(
                broker.tryWriteLock(RangeToken.forWriting(key, value3)));
        finishLatch.countDown();
    }

    @Test
    public void testReadEqualsIsRangeBlockedIfWritingSameValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadGteIsNotRangeBlockedIfWritingLtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                Token token = RangeToken.forWriting(key, ltValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(RangeToken.forReading(key,
                Operator.GREATER_THAN_OR_EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadGteisRangeBlockedIfWritingEqValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(RangeToken.forReading(key,
                Operator.GREATER_THAN_OR_EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadGteisRangeBlockedIfWritingGtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                Token token = RangeToken.forWriting(key, gtValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(RangeToken.forReading(key,
                Operator.GREATER_THAN_OR_EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadGtIsNotRangeBlockedIfWritingEqValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.GREATER_THAN, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadGtIsNotRangeBlockedIfWritingLtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                Token token = RangeToken.forWriting(key, ltValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.GREATER_THAN, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadGtisRangeBlockedIfWritingGtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                Token token = RangeToken.forWriting(key, gtValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.GREATER_THAN, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadLteIsNotRangeBlockedIfWritingGtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                Token token = RangeToken.forWriting(key, gtValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(RangeToken.forReading(key,
                Operator.LESS_THAN_OR_EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadLteisRangeBlockedIfWritingEqValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(RangeToken.forReading(key,
                Operator.LESS_THAN_OR_EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadLteisRangeBlockedIfWritingLtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                Token token = RangeToken.forWriting(key, ltValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(RangeToken.forReading(key,
                Operator.LESS_THAN_OR_EQUALS, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadLtIsNotBlockedIfWritingGtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                Token token = RangeToken.forWriting(key, gtValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.LESS_THAN, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadLtIsNotRangeBlockedIfWritingEqValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.LESS_THAN, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadLtIsRangeBlockedIfWritingLtValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                Token token = RangeToken.forWriting(key, ltValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.LESS_THAN, value)));
        finishLatch.countDown();
    }

    @Test
    public void testReadBwisRangeBlockedIfWritingGtLowerValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtLowerValue = Variables.register("gtLowerValue",
                        increase(value, value1));
                Token token = RangeToken.forWriting(key, gtLowerValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.BETWEEN, value, value1)));
        finishLatch.countDown();
    }

    @Test
    public void testReadBwIsNotRangeBlockedIfWritingLtLowerValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltLowerValue = Variables.register("ltLowerValue",
                        decrease(value));
                Token token = RangeToken.forWriting(key, ltLowerValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.BETWEEN, value, value1)));
        finishLatch.countDown();
    }

    @Test
    public void testReadBwIsRangedBlockedIfWritingEqLowerValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.BETWEEN, value, value1)));
        finishLatch.countDown();
    }

    @Test
    public void testReadBwIsRangedBlockedIfWritingLtHigherValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltHigherValue = Variables.register("ltHigherValue",
                        decrease(value1, value));
                Token token = RangeToken.forWriting(key, ltHigherValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.BETWEEN, value, value1)));
        finishLatch.countDown();
    }

    @Test
    public void testReadBwIsNotRangeBlockedIfWritingEqHigherValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forWriting(key, value1);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.BETWEEN, value, value1)));
        finishLatch.countDown();
    }

    @Test
    public void testReadBwIsNotRangeBlockedIfWritingGtHigherValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtHigherValue = Variables.register("gtHigherValue",
                        increase(value1));
                Token token = RangeToken.forWriting(key, gtHigherValue);
                Permit permit = broker.writeLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNotNull(broker.tryReadLock(
                RangeToken.forReading(key, Operator.BETWEEN, value, value1)));
        finishLatch.countDown();
    }

    @Test
    public void testWriteIsRangeBlockedIfReadingEqualValue()
            throws InterruptedException {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Token token = RangeToken.forReading(key, Operator.EQUALS,
                        value);
                Permit permit = broker.readLock(token);
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                permit.release();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertNull(
                broker.tryWriteLock(RangeToken.forWriting(key, value)));
        finishLatch.countDown();
    }

    @Test
    public void testSameThreadNotRangeBlockedIfReadingRangeThatCoversHeldWrite() {
        Permit permit = broker.writeLock(RangeToken.forWriting(Text.wrap("foo"),
                Value.wrap(Convert.javaToThrift(10))));
        Assert.assertNotNull(broker
                .tryWriteLock(RangeToken.forReading(Text.wrapCached("foo"),
                        Operator.BETWEEN, Value.wrap(Convert.javaToThrift(5)),
                        Value.wrap(Convert.javaToThrift(15)))));
        permit.release();
    }

    @Test
    public void testWriteNotRangeBlockedIfNoReading() {
        Text key = Variables.register("key", TestData.getText());
        Value value = Variables.register("value", TestData.getValue());
        Assert.assertNotNull(
                broker.tryWriteLock(RangeToken.forWriting(key, value)));
    }

    @Test
    public void testRangeLockUnlock() throws InterruptedException {
        Text key = Text.wrap("foo");
        Value value = Value.wrap(Convert.javaToThrift(17));
        RangeToken token = RangeToken.forReading(key, Operator.GREATER_THAN,
                value);
        Permit permit = broker.readLock(token);
        RangeToken token2 = RangeToken.forWriting(key, increase(value));
        Assert.assertNull(broker.tryWriteLock(token2));
        Stopwatch watch = Stopwatch.createUnstarted();
        Thread t = new Thread(() -> {
            watch.start();
            Permit p = broker.writeLock(token2);
            watch.stop();
            Assert.assertEquals(token2, p.token());
            p.release();
        });
        t.start();
        int sleep = 500;
        Threads.sleep(sleep);
        permit.release();
        t.join();
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        Assert.assertTrue(elapsed >= sleep);
    }

    @Test
    public void testRangeLocksKeepTrackOfDistinctRangesEvenWhenConnected() {
        Text key = Text.wrap("foo");
        Permit a = broker.readLock(RangeToken.forReading(key,
                Operator.LESS_THAN, Value.wrap(Convert.javaToThrift(10))));
        Permit b = broker.readLock(RangeToken.forReading(key,
                Operator.GREATER_THAN, Value.wrap(Convert.javaToThrift(5))));
        RangeToken token = RangeToken.forWriting(key,
                Value.wrap(Convert.javaToThrift(7)));
        Assert.assertNull(broker.tryWriteLock(token));
        b.release();
        Assert.assertNull(broker.tryWriteLock(token));
        a.release();
    }

    @Test
    public void testRangeWriteLockIsExclusive() {
        Text key = Text.wrap("foo");
        Value value = Value.wrap(Convert.javaToThrift(1));
        RangeToken token = RangeToken.forWriting(key, value);
        Permit a = broker.writeLock(token);
        Assert.assertNull(broker.tryWriteLock(token));
        a.release();
        Assert.assertNotNull(broker.tryWriteLock(token));
    }

    private Value decrease(Value value) {
        Value lt = null;
        while (lt == null || lt.compareTo(value) >= 0) {
            lt = TestData.getValue();
        }
        return lt;
    }

    private Value decrease(Value value, Value butKeepHigherThan) {
        Value lt = null;
        while (lt == null || lt.compareTo(butKeepHigherThan) <= 0) {
            lt = decrease(value);
        }
        return lt;
    }

    private Value increase(Value value, Value butKeepLowerThan) {
        Value gt = null;
        while (gt == null || gt.compareTo(butKeepLowerThan) >= 0) {
            gt = increase(value);
        }
        return gt;
    }

    /**
     * Return a Value that is greater than {@code value}
     * 
     * @param value
     * @return the greater value
     */
    private Value increase(Value value) {
        Value gt = null;
        while (gt == null || gt.compareTo(value) <= 0) {
            gt = TestData.getValue();
        }
        return gt;
    }

}
