/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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

import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.annotate.Experimental;
import org.cinchapi.concourse.server.model.Text;
import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * 
 * @author jnelson
 */
@Experimental
public class RangeLockTest extends ConcourseBaseTest {
    
    @Test
    public void testWriteLtLowerValueIsNotRangeBlockedIfReadingBw(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value1 = Variables.register("value1", TestData.getValue());
        final Value value2 = Variables.register("value2", increase(value1));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForReading(key, Operator.BETWEEN, value1, value2)
                        .readLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForReading(key, Operator.BETWEEN, value1, value2)
                        .readLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Value value3 = Variables.register("value3", decrease(value1));
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.WRITE, null, key,
                value3));
        flag.set(false);
    }

    @Test
    public void testWriteGtHigherValueIsNotRangeBlockedIfReadingBw() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value1 = Variables.register("value1", TestData.getValue());
        final Value value2 = Variables.register("value2", increase(value1));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForReading(key, Operator.BETWEEN, value1, value2)
                        .readLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForReading(key, Operator.BETWEEN, value1, value2)
                        .readLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Value value3 = Variables.register("value3", increase(value2));
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.WRITE, null, key,
                value3));
        flag.set(false);
    }

    @Test
    public void testReadEqualsIsRangeBlockedIfWritingSameValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadGteIsNotRangeBlockedIfWritingLtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                RangeLock.grabForWriting(key, ltValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, ltValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN_OR_EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadGteisRangeBlockedIfWritingEqValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN_OR_EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadGteisRangeBlockedIfWritingGtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                RangeLock.grabForWriting(key, gtValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, gtValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN_OR_EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadGtIsNotRangeBlockedIfWritingEqValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadGtIsNotRangeBlockedIfWritingLtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                RangeLock.grabForWriting(key, ltValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, ltValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadGtisRangeBlockedIfWritingGtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                RangeLock.grabForWriting(key, gtValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, gtValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLteIsNotRangeBlockedIfWritingGtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                RangeLock.grabForWriting(key, gtValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, gtValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN_OR_EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLteisRangeBlockedIfWritingEqValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN_OR_EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLteisRangeBlockedIfWritingLtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                RangeLock.grabForWriting(key, ltValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, ltValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN_OR_EQUALS, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLtIsNotBlockedIfWritingGtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                RangeLock.grabForWriting(key, gtValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, gtValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLtIsNotRangeBlockedIfWritingEqValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLtIsRangeBlockedIfWritingLtValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltValue = Variables.register("ltValue", decrease(value));
                RangeLock.grabForWriting(key, ltValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, ltValue).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadBwisRangeBlockedIfWritingGtLowerValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtLowerValue = Variables.register("gtLowerValue",
                        increase(value, value1));
                RangeLock.grabForWriting(key, gtLowerValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, gtLowerValue).writeLock()
                        .unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }

    @Test
    public void testReadBwIsNotRangeBlockedIfWritingLtLowerValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltLowerValue = Variables.register("ltLowerValue",
                        decrease(value));
                RangeLock.grabForWriting(key, ltLowerValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, ltLowerValue).writeLock()
                        .unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }

    @Test
    public void testReadBwIsRangedBlockedIfWritingEqLowerValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }

    @Test
    public void testReadBwIsRangedBlockedIfWritingLtHigherValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltHigherValue = Variables.register("ltHigherValue",
                        decrease(value1, value));
                RangeLock.grabForWriting(key, ltHigherValue).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, ltHigherValue).writeLock()
                        .unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }

    @Test
    public void testReadBwIsNotRangeBlockedIfWritingEqHigherValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.grabForWriting(key, value1).writeLock().lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, value1).writeLock().unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }

    @Test
    public void testReadBwIsNotRangeBlockedIfWritingGtHigherValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value getHigherValue = Variables.register("gtHigherValue",
                        increase(value1));
                RangeLock.grabForWriting(key, getHigherValue).writeLock()
                        .lock();
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.grabForWriting(key, getHigherValue).writeLock()
                        .unlock();
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }

    @Test
    public void testWriteIsRangeBlockedIfReadingEqualValue() {
        Text key = Variables.register("key", TestData.getText());
        Value value = Variables.register("value", TestData.getValue());
        RangeLock.grabForReading(key, Operator.EQUALS, value).readLock().lock();
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.WRITE, null, key,
                value));
        RangeLock.grabForReading(key, Operator.EQUALS, value).readLock()
                .unlock();
    }

    @Test
    public void testWriteNotRangeBlockedIfNoReading() {
        Text key = Variables.register("key", TestData.getText());
        Value value = Variables.register("value", TestData.getValue());
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.WRITE, null, key,
                value));
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
