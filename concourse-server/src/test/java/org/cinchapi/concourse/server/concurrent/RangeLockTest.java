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
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 * 
 * @author jnelson
 */
@Experimental
@Ignore
public class RangeLockTest extends ConcourseBaseTest {

    @Test
    public void testReadEqualsIsRangeBlockedIfWritingSameValue() {
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.writeLock(key, value);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value);
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
                RangeLock.writeLock(key, ltValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, ltValue);
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
                RangeLock.writeLock(key, value);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value);
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
                RangeLock.writeLock(key, gtValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, gtValue);
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
                RangeLock.writeLock(key, value);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value);
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
                RangeLock.writeLock(key, ltValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, ltValue);
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
                RangeLock.writeLock(key, gtValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, gtValue);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.GREATER_THAN, key, value));
        flag.set(false);
    }

    @Test
    public void testReadLteIsNotRangeBlockedIfWritingGtValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtValue = Variables.register("gtValue", increase(value));
                RangeLock.writeLock(key, gtValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, gtValue);
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
                RangeLock.writeLock(key, value);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value);
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
                RangeLock.writeLock(key, ltValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, ltValue);
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
                RangeLock.writeLock(key, gtValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, gtValue);
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
                RangeLock.writeLock(key, value);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value);
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
                RangeLock.writeLock(key, ltValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, ltValue);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.LESS_THAN, key, value));
        flag.set(false);
    }
    
    @Test
    public void testReadBwisRangeBlockedIfWritingGtLowerValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value gtLowerValue = Variables.register("gtLowerValue", increase(value));
                RangeLock.writeLock(key, gtLowerValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, gtLowerValue);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }
    
    @Test
    public void testReadBwIsNotRangeBlockedIfWritingLtLowerValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltLowerValue = Variables.register("ltLowerValue", decrease(value));
                RangeLock.writeLock(key, ltLowerValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, ltLowerValue);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }
    
    @Test
    public void testReadBwIsRangedBlockedIfWritingEqLowerValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.writeLock(key, value);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }
    
    @Test
    public void testReadBwIsRangedBlockedIfWritingLtHigherValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value ltHigherValue = Variables.register("ltHigherValue", decrease(value1));
                RangeLock.writeLock(key, ltHigherValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, ltHigherValue);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }
    
    @Test
    public void testReadBwIsNotRangeBlockedIfWritingEqHigherValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                RangeLock.writeLock(key, value1);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, value1);
            }

        });
        t.start();
        TestData.sleep(); // need to sleep because of thread start overhead
        Assert.assertFalse(RangeLock.isRangeBlocked(LockType.READ,
                Operator.BETWEEN, key, value, value1));
        flag.set(false);
    }
    
    @Test
    public void testReadBwIsNotRangeBlockedIfWritingGtHigherValue(){
        final Text key = Variables.register("key", TestData.getText());
        final Value value = Variables.register("value", TestData.getValue());
        final Value value1 = Variables.register("value1", increase(value));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                Value getHigherValue = Variables.register("gtHigherValue", increase(value1));
                RangeLock.writeLock(key, getHigherValue);
                while (flag.get() == true) {
                    continue;
                }
                RangeLock.writeUnlock(key, getHigherValue);
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
        RangeLock.readLock(Operator.EQUALS, key, value);
        Assert.assertTrue(RangeLock.isRangeBlocked(LockType.WRITE, null, key,
                value));
        RangeLock.readUnlock(Operator.EQUALS, key, value);
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
        while (lt == null || lt.compareTo(value) != -1) {
            lt = TestData.getValue();
        }
        return lt;
    }

    /**
     * Return a Value that is greater than {@code value}
     * 
     * @param value
     * @return the greater value
     */
    private Value increase(Value value) {
        Value gt = null;
        while (gt == null || gt.compareTo(value) != 1) {
            gt = TestData.getValue();
        }
        return gt;
    }

}
