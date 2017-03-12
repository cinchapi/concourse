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
package com.cinchapi.concourse.server.storage;

import java.io.File;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.AtomicStateException;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.Transaction;
import com.cinchapi.concourse.server.storage.TransactionStateException;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for {@link Transaction}.
 * 
 * @author Jeff Nelson
 */
public class TransactionTest extends AtomicOperationTest {

    private String directory;

    @Rule
    public TestWatcher w = new TestWatcher() {
        @Override
        protected void starting(Description desc) {
            store.stop(); // Stop the engine so that data isn't transported in
                          // the middle of a test.
        }
    };

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(directory);
    }

    @Override
    protected Engine getDestination() {
        directory = TestData.DATA_DIR + File.separator + Time.now();
        Engine engine = new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
        engine.start(); // Start the engine manually because
                        // AtomicOperation#start does not do it
        return engine;
    }

    @Override
    protected Transaction getStore() {
        destination = getDestination();
        return Transaction.start((Engine) destination);
    }

    @Test(expected = TransactionStateException.class)
    public void testAtomicOperationFromTransactionFailsIfVersionChangesWithCommit() {
        Transaction txn = (Transaction) this.store;
        AtomicOperation operation = txn.startAtomicOperation();
        operation.select("foo", 1);
        Engine engine = (Engine) this.destination;
        operation.commit();
        engine.add("foo", Convert.javaToThrift("bar"), 1);
        Assert.assertFalse(txn.commit());
    }

    public void testAtomicOperationFromTransactionWontFailIfVersionChangesWithoutCommit() {
        Transaction txn = (Transaction) this.store;
        AtomicOperation operation = txn.startAtomicOperation();
        operation.select("foo", 1);
        Engine engine = (Engine) this.destination;
        engine.add("foo", Convert.javaToThrift("bar"), 1);
        operation.abort();
        Assert.assertTrue(txn.commit());
    }

    @Test(expected = TransactionStateException.class)
    public void testNoChangesPersistOnFailure() {
        super.testNoChangesPersistOnFailure();
    }

    @Test(expected = TransactionStateException.class)
    public void testCommitFailsIfVersionChanges() {
        super.testCommitFailsIfVersionChanges();
    }

    @Test(expected = TransactionStateException.class)
    public void testFailureIfWriteToRecordThatIsRead()
            throws InterruptedException {
        super.testFailureIfWriteToRecordThatIsRead();
    }

    @Test(expected = TransactionStateException.class)
    public void testFailureIfWriteToKeyInRecordThatIsRead()
            throws InterruptedException {
        super.testFailureIfWriteToKeyInRecordThatIsRead();
    }

    @Test
    public void testFailedAtomicOperationDoesNotKillTransaction() {
        Transaction transaction = (Transaction) store;
        AtomicOperation operation = transaction.startAtomicOperation();
        operation.select(1);
        transaction.destination.accept(Write.add("foo",
                Convert.javaToThrift(1), 1));
        try {
            Assert.assertFalse(operation.commit());
            operation.abort();
        }
        catch (AtomicStateException e) {
            operation.abort();
        }
        Assert.assertTrue(transaction.commit());
    }

    @Test
    public void testFailedAtomicOperationWillThrowAtomicStateExceptionButTransactionWontFail() {
        Transaction transaction = (Transaction) store;
        AtomicOperation operation = transaction.startAtomicOperation();
        operation.select("foo", 1);
        transaction.destination.accept(Write.add("foo",
                Convert.javaToThrift(1), 1));
        try {
            operation.select(2);
            Assert.fail();
        }
        catch (AtomicStateException e) {
            Assert.assertTrue(transaction.commit());
        }
    }

    @Test
    public void testTransactionIsntEverPenalizedForFailedAtomicOperationThatDidntCommit() {
        Transaction transaction = (Transaction) store;
        AtomicOperation operation = transaction.startAtomicOperation();
        operation.select("foo", 1);
        transaction.destination.accept(Write.add("foo",
                Convert.javaToThrift(1), 1));
        transaction.destination.accept(Write.add("foo",
                Convert.javaToThrift(2), 1));
        int count = TestData.getScaleCount();
        for(int i = 0; i < count; i++){
            transaction.destination.accept(Write.add("foo",
                    Convert.javaToThrift(i), 1));
        }
        Assert.assertTrue(transaction.commit());
    }

}
