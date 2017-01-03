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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Lists;

/**
 * Unit tests for an {@link AtomicOperation} that commits to the {@link Engine}
 * 
 * @author Jeff Nelson
 */
public class EngineAtomicOperationTest extends AtomicOperationTest {

    private String directory;

    @Rule
    public TestWatcher w = new TestWatcher() {
        @Override
        protected void starting(Description desc) {
            store.stop(); // Stop the engine so that data isn't transported in
                          // the middle of a test.
        }
    };

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

    @Test
    public void testEngineDoesNotMissAnyVersionChangeNotifications() {
        final String key = "foo";
        final long record = 1;
        final AtomicBoolean aRunning = new AtomicBoolean(true);
        final AtomicBoolean bRunning = new AtomicBoolean(true);
        final AtomicBoolean aDone = new AtomicBoolean(false);
        final AtomicBoolean bDone = new AtomicBoolean(false);

        // A thread that continuously modifies the version for key/record
        Thread a = new Thread(new Runnable() {

            @Override
            public void run() {
                int count = 0;
                while (aRunning.get()) {
                    destination.accept(Write.add(key,
                            Convert.javaToThrift(count), record));
                    count++;
                }
                aDone.set(true);
            }

        });

        // A thread that continuously creates atomic operations and registers
        // them as version change listeners for key/record
        final List<AtomicOperation> operations = Variables.register(
                "operations", Lists.<AtomicOperation> newArrayList());
        Thread b = new Thread(new Runnable() {

            @Override
            public void run() {
                while (bRunning.get()) {
                    AtomicOperation operation = destination
                            .startAtomicOperation();
                    operations.add(operation);
                    operation.select(key, record);
                }
                bDone.set(true);
            }

        });

        a.start();
        b.start();
        TestData.sleep();
        bRunning.set(false);
        while (!bDone.get()) {
            continue;
        }
        aRunning.set(false);
        while (!aDone.get()) {
            continue;
        }
        int i = 0;
        Variables.register("size", operations.size());
        for (AtomicOperation operation : operations) {
            Variables.register("operation_" + i, operation);
            Assert.assertFalse(operation.open.get()); // ensure that all the
                                                      // atomic operations were
                                                      // notified about the
                                                      // version change
            i++;
        }
        destination.stop();

    }

    @Test(expected = AtomicStateException.class)
    public void testNoPhantomReadWithTimestampInTheFutureUsingSelect() {
        long aheadOfTime = Time.now() + (long) 10e9;
        String key = Variables.register("key", TestData.getSimpleString());
        TObject value = Variables.register("value", TestData.getTObject());
        long record = Variables.register("record", TestData.getLong());

        AtomicOperation atomicOp = (AtomicOperation) store;
        atomicOp.select(record, aheadOfTime);
        destination.accept(Write.add(key, value, record));
        atomicOp.select(record, aheadOfTime);
    }

    @Test(expected = AtomicStateException.class)
    public void testNoPhantomReadWithTimestampInTheFutureUsingSelectWithKey() {
        long aheadOfTime = Time.now() + (long) 10e9;
        String key = Variables.register("key", TestData.getSimpleString());
        TObject value = Variables.register("value", TestData.getTObject());
        long record = Variables.register("record", TestData.getLong());

        AtomicOperation atomicOp = (AtomicOperation) store;
        atomicOp.select(key, record, aheadOfTime);
        destination.accept(Write.add(key, value, record));
        atomicOp.select(key, record, aheadOfTime);
    }

    @Test(expected = AtomicStateException.class)
    public void testNoPhantomReadWithTimestampInTheFutureUsingVerify() {
        long aheadOfTime = Time.now() + (long) 10e9;
        String key = Variables.register("key", TestData.getSimpleString());
        TObject value = Variables.register("value", TestData.getTObject());
        long record = Variables.register("record", TestData.getLong());

        AtomicOperation atomicOp = (AtomicOperation) store;
        atomicOp.verify(key, value, record, aheadOfTime);
        destination.accept(Write.add(key, value, record));
        atomicOp.verify(key, value, record, aheadOfTime);
    }

    @Test(expected = AtomicStateException.class)
    public void testNoPhantomReadWithTimestampInTheFutureUsingBrowse() {
        long aheadOfTime = Time.now() + (long) 10e9;
        String key = Variables.register("key", TestData.getSimpleString());
        TObject value = Variables.register("value", TestData.getTObject());
        long record = Variables.register("record", TestData.getLong());

        AtomicOperation atomicOp = (AtomicOperation) store;
        atomicOp.browse(key, aheadOfTime);
        destination.accept(Write.add(key, value, record));
        atomicOp.browse(key, aheadOfTime);
    }

    @Test(expected = AtomicStateException.class)
    public void testNoPhantomReadWithTimestampInTheFutureUsingDoExplore() {
        long aheadOfTime = Time.now() + (long) 10e9;
        String key = Variables.register("key", TestData.getSimpleString());
        TObject value = Convert.javaToThrift(50);
        long record = Variables.register("record", TestData.getLong());

        AtomicOperation atomicOp = (AtomicOperation) store;
        atomicOp.doExplore(aheadOfTime, key, Operator.BETWEEN,
                Convert.javaToThrift(0), Convert.javaToThrift(100));
        destination.accept(Write.add(key, value, record));
        atomicOp.doExplore(aheadOfTime, key, Operator.BETWEEN,
                Convert.javaToThrift(0), Convert.javaToThrift(100));
    }

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

}
