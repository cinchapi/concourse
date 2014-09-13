/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.cinchapi.concourse.server.io.FileSystem;
import org.cinchapi.concourse.server.storage.temp.Write;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Lists;

/**
 * Unit tests for an {@link AtomicOperation} that commits to the {@link Engine}
 * 
 * @author jnelson
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
                    AtomicOperation operation = AtomicOperation
                            .start(destination);
                    operations.add(operation);
                    operation.fetch(key, record);
                }
                bDone.set(true);
            }

        });

        a.start();
        b.start();
        TestData.sleep();
        bRunning.set(false);
        while(!bDone.get()){
            continue;
        }
        aRunning.set(false);
        while (!aDone.get()) {
            continue;
        }
        int i = 0 ;
        Variables.register("size", operations.size());
        for (AtomicOperation operation : operations) {
            Variables.register("operation_"+i, operation);
            Assert.assertFalse(operation.open); // ensure that all the atomic
                                                // operations were notified
                                                // about the version change
            i++;
        }
        destination.stop();

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
