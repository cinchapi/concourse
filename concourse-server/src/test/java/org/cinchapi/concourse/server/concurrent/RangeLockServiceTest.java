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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RangeLockService}.
 * 
 * @author jnelson
 */
public class RangeLockServiceTest extends ConcourseBaseTest {

    private RangeLockService rangeLockService;

    @Override
    protected void beforeEachTest() {
        rangeLockService = RangeLockService.create();
    }

    @Test
    public void testWriteIsRangeBlockedIfReadingAllValues() {
        ReadLock readLock = rangeLockService.getReadLock(RangeToken.forReading(
                Text.wrapCached("foo"), Operator.BETWEEN,
                Value.NEGATIVE_INFINITY, Value.POSITIVE_INFINITY));
        readLock.lock();
        Assert.assertTrue(rangeLockService.isRangeBlocked(
                LockType.RANGE_WRITE,
                RangeToken.forWriting(Text.wrapCached("foo"),
                        TestData.getValue())));
        readLock.unlock();
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
                        ReadLock readLock = rangeLockService.getReadLock("foo",
                                Operator.EQUALS, Convert.javaToThrift(1));
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
                        WriteLock writeLock = rangeLockService.getWriteLock(
                                "foo", Convert.javaToThrift(1));
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
                rangeLockService.getReadLock(key, Operator.BETWEEN, value1,
                        value2).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getReadLock(key, Operator.BETWEEN, value1,
                        value2).unlock();
            }

        });
        t.start();
        startLatch.await();
        Value value3 = Variables.register("value3", decrease(value1));
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.WRITE,
                RangeToken.forReading(key, null, value3)));
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
                rangeLockService.getReadLock(key, Operator.BETWEEN, value1,
                        value2).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getReadLock(key, Operator.BETWEEN, value1,
                        value2).unlock();
            }

        });
        t.start();
        startLatch.await();
        Value value3 = Variables.register("value3", increase(value2));
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.WRITE,
                RangeToken.forWriting(key, value3)));
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
                rangeLockService.getWriteLock(key, value).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, ltValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, ltValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
                RangeToken.forReading(key, Operator.GREATER_THAN_OR_EQUALS,
                        value)));
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
                rangeLockService.getWriteLock(key, value).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
                RangeToken.forReading(key, Operator.GREATER_THAN_OR_EQUALS,
                        value)));
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
                rangeLockService.getWriteLock(key, gtValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, gtValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
                RangeToken.forReading(key, Operator.GREATER_THAN_OR_EQUALS,
                        value)));
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
                rangeLockService.getWriteLock(key, value).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, ltValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, ltValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, gtValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, gtValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, gtValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, gtValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService
                .isRangeBlocked(LockType.READ, RangeToken.forReading(key,
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
                rangeLockService.getWriteLock(key, value).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService
                .isRangeBlocked(LockType.READ, RangeToken.forReading(key,
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
                rangeLockService.getWriteLock(key, ltValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, ltValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService
                .isRangeBlocked(LockType.READ, RangeToken.forReading(key,
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
                rangeLockService.getWriteLock(key, gtValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, gtValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, value).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, ltValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, ltValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, gtLowerValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, gtLowerValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, ltLowerValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, ltLowerValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, value).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, ltHigherValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, ltHigherValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getWriteLock(key, value1).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, value1).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                Value getHigherValue = Variables.register("gtHigherValue",
                        increase(value1));
                rangeLockService.getWriteLock(key, getHigherValue).lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getWriteLock(key, getHigherValue).unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.READ,
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
                rangeLockService.getReadLock(key, Operator.EQUALS, value)
                        .lock();
                startLatch.countDown();
                try {
                    finishLatch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rangeLockService.getReadLock(key, Operator.EQUALS, value)
                        .unlock();
            }

        });
        t.start();
        startLatch.await();
        Assert.assertTrue(rangeLockService.isRangeBlocked(LockType.WRITE,
                RangeToken.forWriting(key, value)));
        finishLatch.countDown();
    }

    @Test
    public void testWriteNotRangeBlockedIfNoReading() {
        Text key = Variables.register("key", TestData.getText());
        Value value = Variables.register("value", TestData.getValue());
        Assert.assertFalse(rangeLockService.isRangeBlocked(LockType.WRITE,
                RangeToken.forWriting(key, value)));
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
