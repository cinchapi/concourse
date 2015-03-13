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
package org.cinchapi.concourse.server.storage;

import java.io.File;

import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Unit tests for {@link Transaction}.
 * 
 * @author jnelson
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
        operation.fetch("foo", 1);
        Engine engine = (Engine) this.destination;
        operation.commit();
        engine.add("foo", Convert.javaToThrift("bar"), 1);
        Assert.assertFalse(txn.commit());
    }

    public void testAtomicOperationFromTransactionWontFailIfVersionChangesWithoutCommit() {
        Transaction txn = (Transaction) this.store;
        AtomicOperation operation = txn.startAtomicOperation();
        operation.fetch("foo", 1);
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
        operation.browse(1);
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

}
