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
package com.cinchapi.concourse.server.storage;

import java.io.File;

import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for a nested {@link AtomicOperation} that commits to another
 * {@link AtomicOperation}
 *
 * @author Jeff Nelson
 */
public class NestedAtomicOperationTest extends AtomicOperationTest {

    private String directory;
    private Engine engine;

    @Override
    protected final AtomicSupport getDestination() {
        directory = TestData.DATA_DIR + File.separator + Time.now();
        engine = new Engine(directory + File.separator + "buffer",
                directory + File.separator + "database");
        engine.start(); // Start the engine manually because
                        // AtomicOperation#start does not do it
        return getDestination(engine);
    }

    /**
     * Return a destination that is sourced from the {@code engine}.
     * 
     * @param engine
     * @return the destination
     */
    protected AtomicSupport getDestination(Engine engine) {
        return engine.startAtomicOperation();
    }

    @Override
    protected void cleanup(Store store) {
        engine.stop();
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

    @Test
    @Ignore
    public void testRangeReadInterruptedByWrite() {
        // This test knowingly fails for Transaction Atomic Operations
    }

    @Test
    @Ignore
    public void testAllAtomicOperationsEventuallyTerminate() {
        // This test knowing fails for Transaction Atomic Operations because it
        // does not support spawning multiple concurrent atomic operations.
    }

}
