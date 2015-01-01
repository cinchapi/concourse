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
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.TestData;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit tests for an {@link AtomicOperation} that commits to a
 * {@link Transaction}
 * 
 * @author jnelson
 */
public class TransactionAtomicOperationTest extends AtomicOperationTest {

    private String directory;

    @Override
    protected Transaction getDestination() {
        directory = TestData.DATA_DIR + File.separator + Time.now();
        Engine engine = new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
        engine.start(); // Start the engine manually because
                        // AtomicOperation#start does not do it
        return Transaction.start(engine);
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(directory);
    }

    @Test
    @Ignore
    public void testNoChangesPersistOnFailure() {
        // This test does not make sense since Transactions and their spawned
        // atomic operations are isolated
    }

    @Test
    @Ignore
    public void testCommitFailsIfVersionChanges() {
        // This test does not make sense since Transactions and their spawned
        // atomic operations are isolated
    }

    @Test
    @Ignore
    public void testFailureIfWriteToRecordThatIsRead()
            throws InterruptedException {
        // This test does not make sense since Transactions and their spawned
        // atomic operations are isolated
    }

    @Test
    @Ignore
    public void testOnlyOneSuccessDuringRaceConditionWithConflict()
            throws InterruptedException {
        // This test does not make sense since Transactions and their spawned
        // atomic operations are isolated
    }

    @Test
    @Ignore
    public void testFailureIfWriteToKeyInRecordThatIsRead()
            throws InterruptedException {
        // This test does not make sense since Transactions and their spawned
        // atomic operations are isolated
    }

}
