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

import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.AtomicOperation;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.server.storage.Transaction;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for an {@link AtomicOperation} that commits to a
 * {@link Transaction}
 * 
 * @author Jeff Nelson
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
