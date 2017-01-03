/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.AtomicSupport;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link AtomicOperation}.
 * 
 * @author Jeff Nelson
 */
public abstract class AtomicOperationTest extends BufferedStoreTest {

    protected AtomicSupport destination;

    @Test
    public void testNoDeadlockIfAddToKeyAsValueBeforeFindingEqKeyAndValue() {
        long record = TestData.getLong();
        String key = "foo";
        TObject value = Convert.javaToThrift("bar");
        add(key, value, record);
        store.find(key, Operator.EQUALS, value);
        Assert.assertTrue(((AtomicOperation) store).commit());
    }

    @Test
    public void testNoDeadlockIfAddToKeyAsValueBeforeFindingGttKeyAndValue() {
        long record = TestData.getLong();
        String key = "foo";
        TObject value = Convert.javaToThrift("bar");
        add(key, value, record);
        store.find(key, Operator.GREATER_THAN, value);
        Assert.assertTrue(((AtomicOperation) store).commit());
    }

    @Test
    public void testNoDeadlockIfAddToKeyAsValueBeforeFindingBwtKeyAndValue() {
        long record = TestData.getLong();
        String key = "foo";
        TObject value = Convert.javaToThrift("bar");
        add(key, value, record);
        store.find(key, Operator.BETWEEN, value, Convert.javaToThrift("bars"));
        Assert.assertTrue(((AtomicOperation) store).commit());
    }

    @Test
    public void testAbort() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        Assert.assertTrue(store.verify(key, value, record));
        ((AtomicOperation) store).abort();
        Assert.assertFalse(destination.verify(key, value, record));
    }

    @Test
    public void testCommit() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        add(key, value, record);
        ((AtomicOperation) store).commit();
        Assert.assertTrue(destination.verify(key, value, record));
    }

    @Test
    public void testCommitFailsIfVersionChanges() {
        String key = Variables.register("key", TestData.getSimpleString());
        TObject value = Variables.register("value", TestData.getTObject());
        long record = Variables.register("record", TestData.getLong());
        add(key, value, record);
        AtomicOperation other = destination.startAtomicOperation();
        other.add(key, value, record);
        Assert.assertTrue(other.commit());
        Assert.assertFalse(((AtomicOperation) store).commit());
    }

    @Test
    public void testCommitSucceedsIfChangeIsMadeToRecordInDiffKey() {
        // CON-20
        long record = 1;
        String keyA = "keyA";
        TObject valueA = Convert.javaToThrift("valueA");
        String keyB = "keyB";
        TObject valueB = Convert.javaToThrift("valueB");
        add(keyA, valueA, 1);
        AtomicOperation other = destination.startAtomicOperation();
        other.add(keyB, valueB, record);
        Assert.assertTrue(other.commit());
        Assert.assertTrue(((AtomicOperation) store).commit());
    }

    @Test
    public void testFailureIfWriteToKeyInRecordThatIsRead()
            throws InterruptedException {
        final String key = TestData.getSimpleString();
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.select(key, record);
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
    public void testFailureIfWriteToRecordThatIsRead()
            throws InterruptedException {
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.describe(record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(TestData.getSimpleString(),
                        TestData.getTObject(), record));

            }

        });
        thread.start();
        thread.join();
        Assert.assertFalse(operation.commit());
    }

    @Test
    public void testImmediateVisibility() {
        String key = TestData.getSimpleString();
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
        Assert.assertEquals(Sets.newHashSet(), destination.select(key, record));
        ((AtomicOperation) store).commit();
        Assert.assertEquals(values, destination.select(key, record));
    }

    @Test
    public void testIsolation() {
        AtomicOperation a = destination.startAtomicOperation();
        AtomicOperation b = destination.startAtomicOperation();
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        Assert.assertTrue(((AtomicOperation) a).add(key, value, record));
        Assert.assertTrue(((AtomicOperation) b).add(key, value, record));
        Assert.assertFalse(destination.verify(key, value, record));
    }

    @Test
    public void testNoChangesPersistOnFailure() {
        int count = TestData.getScaleCount();
        String key0 = "";
        for (int i = 0; i < count; i++) {
            String key = TestData.getSimpleString();
            if(i == 0) {
                key0 = key;
            }
            TObject value = TestData.getTObject();
            ((AtomicOperation) store).add(key, value, i);
        }
        destination.accept(Write.add(key0, Convert.javaToThrift("foo"), 0));
        ((AtomicOperation) store).commit();
        for (int i = 1; i < count; i++) {
            Assert.assertTrue(destination.audit(i).isEmpty());
        }
    }

    @Test
    public void testLockUpgrade() {
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();
        store.verify(key, value, record);
        add(key, value, record);
        Assert.assertTrue(((AtomicOperation) store).commit());
    }

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
                try {
                    aSuccess.set(a.commit());
                }
                catch (AtomicStateException e) {} // swallow
            }

        });
        Thread bThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    bSuccess.set(b.commit());
                }
                catch (AtomicStateException e) {} // swallow
            }

        });
        aThread.start();
        bThread.start();
        aThread.join();
        bThread.join();
        Assert.assertTrue((aSuccess.get() && !bSuccess.get())
                || (!aSuccess.get() && bSuccess.get()));
    }

    @Test
    public void testSucceessIfNoWriteToKeyInRecordThatIsRead()
            throws InterruptedException {
        final String key = TestData.getSimpleString();
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.select(key, record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(key + TestData.getSimpleString(),
                        TestData.getTObject(), record));

            }

        });
        thread.start();
        thread.join();
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testSuccessIfNoWriteToRecordThatIsRead()
            throws InterruptedException {
        final long record = TestData.getLong();
        AtomicOperation operation = (AtomicOperation) store;
        operation.describe(record);
        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                destination.accept(Write.add(TestData.getSimpleString(),
                        TestData.getTObject(), record + 1));

            }

        });
        thread.start();
        thread.join();
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindEqOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.EQUALS, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindGtOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.GREATER_THAN, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindGteOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.GREATER_THAN_OR_EQUALS, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindLteOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.LESS_THAN_OR_EQUALS, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindLtOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.LESS_THAN, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindBwOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.BETWEEN, value, Convert.javaToThrift(3));
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindRegexOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.REGEX, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test
    public void testNoDeadLockIfFindNotRegexOnKeyBeforeAddingToKey() {
        String key = "ipeds_id";
        TObject value = Convert.javaToThrift(1);
        long record = Time.now();
        AtomicOperation operation = (AtomicOperation) store;
        operation.find(key, Operator.NOT_REGEX, value);
        operation.add(key, value, record);
        Assert.assertTrue(operation.commit());
    }

    @Test(expected = AtomicStateException.class)
    public void testCannotOperateOnClosedAtomicOperation() {
        AtomicOperation operation = (AtomicOperation) store;
        operation.commit();
        operation.audit(1);
    }

    @Override
    protected void add(String key, TObject value, long record) {
        ((AtomicOperation) store).add(key, value, record);
    }

    protected abstract AtomicSupport getDestination();

    @Override
    protected AtomicOperation getStore() {
        destination = getDestination();
        return destination.startAtomicOperation();
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        ((AtomicOperation) store).remove(key, value, record);

    }

    private AtomicOperation doTestOnlyOneSuccessDuringRaceCondition() {
        AtomicOperation operation = destination.startAtomicOperation();
        for (int i = 0; i < 1; i++) {
            operation.add(TestData.getSimpleString(), TestData.getTObject(), i);
        }
        return operation;
    }

}
