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
package org.cinchapi.concourse.server.storage;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * 
 * 
 * @author jnelson
 */
public abstract class AtomicOperationTest extends BufferedStoreTest {

    private Compoundable destination;

    @Test
    public void testOnlyOneSuccessDuringRaceConditionWithConflict()
            throws InterruptedException {
        final AtomicOperation a = doTestOnlyOneSuccessDuringRaceCondition();
        final AtomicOperation b = doTestOnlyOneSuccessDuringRaceCondition();
        a.add("foo", Convert.javaToThrift("bar"), 1000);
        b.add("foo", Convert.javaToThrift("bar"), 1000);
        final AtomicBoolean aSuccess = new AtomicBoolean(false);
        final AtomicBoolean bSuccess = new AtomicBoolean(false);
        Thread aThread = new Thread(new Runnable() {

            @Override
            public void run() {
                aSuccess.set(a.commit());

            }

        });
        Thread bThread = new Thread(new Runnable() {

            @Override
            public void run() {
                bSuccess.set(b.commit());

            }

        });
        aThread.start();
        bThread.start();
        aThread.join();
        bThread.join();
        Assert.assertTrue((aSuccess.get() && !bSuccess.get())
                || (!aSuccess.get() && bSuccess.get()));
    }

    private AtomicOperation doTestOnlyOneSuccessDuringRaceCondition() {
        AtomicOperation operation = AtomicOperation.start(destination);
        for (int i = 0; i < 1; i++) {
            operation.add(TestData.getString(), TestData.getTObject(), i);
        }
        return operation;
    }

    @Test
    public void testAbort() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        Assert.assertTrue(store.verify(key, value, record));
        ((AtomicOperation) store).abort();
        Assert.assertFalse(destination.verify(key, value, record));
    }

    @Test(expected = AtomicStateException.class)
    public void testFailureIfWriteToRecordThatIsRead()
            throws InterruptedException {
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.describe(record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(TestData.getString(),
                        TestData.getTObject(), record));

            }

        });
        thread.start();
        thread.join();
        Assert.assertFalse(operation.commit());
    }
    
    public void testSuccessIfNoWriteToRecordThatIsRead()
            throws InterruptedException {
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.describe(record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(TestData.getString(),
                        TestData.getTObject(), record+1));

            }

        });
        thread.start();
        thread.join();
        Assert.assertTrue(operation.commit());
    }

    @Test(expected = AtomicStateException.class)
    public void testFailureIfWriteToKeyInRecordThatIsRead()
            throws InterruptedException {
        final String key = TestData.getString();
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.fetch(key, record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(key, TestData.getTObject(), record));

            }

        });
        thread.start();
        thread.join();
        Assert.assertFalse(operation.commit());
    }

    @Test
    public void testSucceessIfNoWriteToKeyInRecordThatIsRead()
            throws InterruptedException {
        final String key = TestData.getString();
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.fetch(key, record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(key + TestData.getString(),
                        TestData.getTObject(), record));

            }

        });
        thread.start();
        thread.join();
        Assert.assertTrue(operation.commit());
    }

    // @Test
    // public void testNoChangesPersistOnFailure(){
    // for(int i = 0; i < TestData.getScaleCount(); i++){
    // String key = TestData.getString();
    // TObject value = TestData.getTObject();
    // ((AtomicOperation) store).add(key, value, i);
    // }
    // }

    @Test
    public void testIsolation() {
        AtomicOperation a = AtomicOperation.start(destination);
        AtomicOperation b = AtomicOperation.start(destination);
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertTrue(((AtomicOperation) a).add(key, value, record));
        Assert.assertTrue(((AtomicOperation) b).add(key, value, record));
        Assert.assertFalse(destination.verify(key, value, record));
    }

    @Test
    public void testCommit() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        ((AtomicOperation) store).commit();
        Assert.assertTrue(destination.verify(key, value, record));
    }

    @Test(expected = AtomicStateException.class)
    public void testCommitFailsIfVersionChanges() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        AtomicOperation other = AtomicOperation.start(destination);
        other.add(key, value, record);
        Assert.assertTrue(other.commit());
        Assert.assertFalse(((AtomicOperation) store).commit());
    }

    @Test
    public void testImmediateVisibility() {
        String key = TestData.getString();
        long record = TestData.getLong();
        Set<TObject> values = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            TObject value = TestData.getTObject();
            while (values.contains(value)) {
                value = TestData.getTObject();
            }
            values.add(value);
            add(key, value, record);
        }
        Assert.assertEquals(Sets.newHashSet(), destination.fetch(key, record));
        ((AtomicOperation) store).commit();
        Assert.assertEquals(values, destination.fetch(key, record));
    }

    @Test
    public void testLockUpgrade() {
        String key = TestData.getString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        store.verify(key, value, record);
        add(key, value, record);
        Assert.assertTrue(((AtomicOperation) store).commit());
    }

    @Override
    protected void add(String key, TObject value, long record) {
        ((AtomicOperation) store).add(key, value, record);
    }

    protected abstract Compoundable getDestination();

    @Override
    protected AtomicOperation getStore() {
        destination = getDestination();
        return AtomicOperation.start(destination);
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        ((AtomicOperation) store).remove(key, value, record);

    }

}
