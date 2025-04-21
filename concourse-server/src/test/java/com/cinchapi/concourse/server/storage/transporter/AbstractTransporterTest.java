/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.transporter;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.AbstractStoreTest;
import com.cinchapi.concourse.server.storage.Engine;
import com.cinchapi.concourse.server.storage.Store;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.TestData;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import com.cinchapi.concourse.server.concurrent.Threads;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Base class for {@link Transporter} functionality.
 *
 * @author Jeff Nelson
 */
public abstract class AbstractTransporterTest extends AbstractStoreTest {

    @Rule
    public TestRule watcher1 = new TestWatcher() {

        @Override
        protected void starting(Description desc) {
            engine.stop();
            ReentrantReadWriteLock lock = createTransportLock();
            Reflection.set("transportLock", lock, engine); /* (authorized) */
            transporter = createTransporter(engine, lock.writeLock());
            Reflection.set("transporter", transporter,
                    engine); /* (authorized) */
            engine.start();
        }
    };

    protected Engine engine;
    protected Transporter transporter;
    private String bufferDir;
    private String dbDir;

    /**
     * Test that concurrent reads and writes maintain data consistency during
     * transport.
     */
    @Test
    public void testConcurrentReadsAndWritesDuringTransport()
            throws InterruptedException {
        // Create a map to track expected data
        final Map<String, Set<TObject>> expectedData = Maps.newConcurrentMap();
        final Map<Long, Map<String, Set<TObject>>> recordData = Maps
                .newConcurrentMap();
        final AtomicBoolean failed = new AtomicBoolean(false);
        final int numThreads = 5;
        final int operationsPerThread = 100;
        final CountDownLatch latch = new CountDownLatch(numThreads * 2); // Writers
                                                                         // +
                                                                         // readers
        final CyclicBarrier barrier = new CyclicBarrier(numThreads * 2);

        // Create writer threads
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    barrier.await(); // Wait for all threads to be ready
                    writeDataAsync(threadId, operationsPerThread, expectedData,
                            recordData);
                }
                catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace();
                }
                finally {
                    latch.countDown();
                }
            }).start();
        }

        // Create reader threads
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    barrier.await(); // Wait for all threads to be ready
                    Thread.sleep(100);
                    readDataAsync(threadId, operationsPerThread, numThreads);
                }
                catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace();
                }
                finally {
                    latch.countDown();
                }
            }).start();
        }

        // Wait for all threads to complete
        latch.await(30, TimeUnit.SECONDS);

        Assert.assertFalse("Concurrent operations failed", failed.get());

        // Wait for final transport to occur
        waitForTransport();

        // Verify database has segments (transport occurred)
        Database database = Reflection.get("durable", engine);
        List<Segment> segments = Reflection.get("segments", database);
        Assert.assertTrue(
                "No segments found in database after concurrent operations",
                segments.size() > 1);

        // Verify final data consistency
        verifyDataConsistency(recordData);
    }

    /**
     * Test that data consistency is maintained during transport operations.
     */
    @Test
    public void testDataConsistencyDuringTransport() {
        // Create a map to track expected data
        Map<String, Set<TObject>> expectedData = Maps.newHashMap();

        // Add initial data
        for (int i = 0; i < 50; i++) {
            String key = "key" + i;
            TObject value = Convert.javaToThrift("value" + i);
            long record = i;

            engine.add(key, value, record);

            Set<TObject> values = expectedData.getOrDefault(key,
                    Sets.newHashSet());
            values.add(value);
            expectedData.put(key, values);
        }

        // Wait for transport to occur
        waitForTransport();

        // Verify all data is accessible and consistent
        for (Map.Entry<String, Set<TObject>> entry : expectedData.entrySet()) {
            String key = entry.getKey();
            for (TObject value : entry.getValue()) {
                long record = Long.parseLong(key.substring(3)); // Extract
                                                                // record from
                                                                // key
                Assert.assertTrue("Data inconsistency found for " + key,
                        engine.verify(key, value, record));
            }
        }

        // Add more data while transport might be happening
        for (int i = 50; i < 100; i++) {
            String key = "key" + i;
            TObject value = Convert.javaToThrift("value" + i);
            long record = i;

            engine.add(key, value, record);

            Set<TObject> values = expectedData.getOrDefault(key,
                    Sets.newHashSet());
            values.add(value);
            expectedData.put(key, values);
        }

        // Wait for transport to occur again
        waitForTransport();

        // Verify all data is still accessible and consistent
        for (Map.Entry<String, Set<TObject>> entry : expectedData.entrySet()) {
            String key = entry.getKey();
            for (TObject value : entry.getValue()) {
                long record = Long.parseLong(key.substring(3)); // Extract
                                                                // record from
                                                                // key
                Assert.assertTrue(
                        "Data inconsistency found after second transport for "
                                + key,
                        engine.verify(key, value, record));
            }
        }
    }

    /**
     * Test that data is properly transported from buffer to database.
     */
    @Test
    public void testDataTransport() {
        // Add data to the engine
        String key = TestData.getSimpleString();
        TObject value = TestData.getTObject();
        long record = TestData.getLong();

        engine.add(key, value, record);

        // Add enough data to ensure transport occurs
        for (int i = 0; i < 100; i++) {
            engine.add("key" + i, Convert.javaToThrift("value" + i), i);
        }

        // Wait for transport to occur
        waitForTransport();

        // Verify data was transported by checking segments in database
        Database database = Reflection.get("durable", engine);
        List<Segment> segments = Reflection.get("segments", database);

        // Verify there are segments in the database (data was transported)
        Assert.assertTrue("No segments found in database after transport",
                segments.size() > 1);

        // Verify the data is accessible through the engine
        Assert.assertTrue(engine.verify(key, value, record));
    }

    /**
     * Test that data is correctly transported after engine restart.
     */
    @Test
    public void testDataTransportAfterEngineRestart() {
        // Add data to the engine
        Map<String, TObject> testData = Maps.newHashMap();
        for (int i = 0; i < 50; i++) {
            String key = "restart_key_" + i;
            TObject value = Convert.javaToThrift("restart_value_" + i);
            long record = i;

            engine.add(key, value, record);
            testData.put(key, value);
        }

        // Wait for transport to occur
        waitForTransport();

        // Stop and restart the engine
        engine.stop();
        engine.start();

        // Verify data is still accessible after restart
        for (Map.Entry<String, TObject> entry : testData.entrySet()) {
            String key = entry.getKey();
            TObject value = entry.getValue();
            long record = Long.parseLong(key.split("_")[2]);

            Assert.assertTrue("Data not accessible after restart: " + key,
                    engine.verify(key, value, record));
        }

        // Add more data after restart
        Map<String, TObject> newData = Maps.newHashMap();
        for (int i = 50; i < 100; i++) {
            String key = "restart_key_" + i;
            TObject value = Convert.javaToThrift("restart_value_" + i);
            long record = i;

            engine.add(key, value, record);
            newData.put(key, value);
        }

        // Wait for transport to occur again
        waitForTransport();

        // Verify all data is accessible
        for (Map.Entry<String, TObject> entry : newData.entrySet()) {
            String key = entry.getKey();
            TObject value = entry.getValue();
            long record = Long.parseLong(key.split("_")[2]);

            Assert.assertTrue(
                    "New data not accessible after restart and transport: "
                            + key,
                    engine.verify(key, value, record));
        }

        // Verify database has segments (transport occurred)
        Database database = Reflection.get("durable", engine);
        List<Segment> segments = Reflection.get("segments", database);
        Assert.assertTrue("No segments found in database after restart",
                segments.size() > 1);
    }

    /**
     * Test that data is correctly transported when adding and removing the same
     * data.
     */
    @Test
    public void testDataTransportWithAddAndRemove() {
        // Add and immediately remove data
        for (int i = 0; i < 50; i++) {
            String key = "temp_key_" + i;
            TObject value = Convert.javaToThrift("temp_value_" + i);
            long record = i;

            engine.add(key, value, record);
            engine.remove(key, value, record);
        }

        // Add permanent data
        Map<String, TObject> permanentData = Maps.newHashMap();
        for (int i = 0; i < 50; i++) {
            String key = "perm_key_" + i;
            TObject value = Convert.javaToThrift("perm_value_" + i);
            long record = i + 100;

            engine.add(key, value, record);
            permanentData.put(key, value);
        }

        // Wait for transport to occur
        waitForTransport();

        // Verify temporary data is not accessible
        for (int i = 0; i < 50; i++) {
            String key = "temp_key_" + i;
            TObject value = Convert.javaToThrift("temp_value_" + i);
            long record = i;

            Assert.assertFalse("Removed data still accessible: " + key,
                    engine.verify(key, value, record));
        }

        // Verify permanent data is accessible
        for (Map.Entry<String, TObject> entry : permanentData.entrySet()) {
            String key = entry.getKey();
            TObject value = entry.getValue();
            long record = Long.parseLong(key.split("_")[2]) + 100;

            Assert.assertTrue("Permanent data not accessible: " + key,
                    engine.verify(key, value, record));
        }

        // Verify database has segments (transport occurred)
        Database database = Reflection.get("durable", engine);
        List<Segment> segments = Reflection.get("segments", database);
        Assert.assertTrue(
                "No segments found in database after add/remove operations",
                segments.size() > 1);
    }

    @Override
    protected void add(String key, TObject value, long record) {
        engine.add(key, value, record);
    }

    @Override
    protected void cleanup(Store store) {
        FileSystem.deleteDirectory(bufferDir);
        FileSystem.deleteDirectory(dbDir);
    }

    /**
     * Return the {@link Transporter} to use in the tests.
     * <p>
     * Use the {@link Engine engine} to get access to the buffer, database and
     * other components, etc. You must use the provided {@code lock} instead of
     * one that is already instantiated on the {@link Engine engine}
     * </p>
     * 
     * @param engine
     * @param lock
     * @return the transporter.
     */
    protected abstract Transporter createTransporter(Engine engine, Lock lock);

    /**
     * Return the correct lock type of to use for the {@link Transporter} being
     * tested.
     * 
     * @return the lock
     */
    protected abstract ReentrantReadWriteLock createTransportLock();

    @Override
    protected Store getStore() {
        bufferDir = TestData.getTemporaryTestDir();
        dbDir = TestData.getTemporaryTestDir();
        engine = new Engine(bufferDir, dbDir);
        return engine;
    }

    @Override
    protected void remove(String key, TObject value, long record) {
        engine.remove(key, value, record);
    }

    /**
     * Generates a Write object for a given thread and operation.
     *
     * @param threadId the ID of the thread
     * @param operationNum the operation number
     * @param isAdd whether this is an add operation (true) or remove operation
     *            (false)
     * @return a Write object
     */
    private Write generateWrite(int threadId, int operationNum, boolean isAdd) {
        String key = getKey(threadId, operationNum);
        TObject value = getValue(threadId, operationNum);
        long record = getRecord(threadId, operationNum);

        return isAdd ? Write.add(key, value, record)
                : Write.remove(key, value, record);
    }

    /**
     * Helper method to generate a consistent key for a given thread and
     * operation.
     * 
     * @param threadId the ID of the thread
     * @param operationNum the operation number
     * @return a consistent key
     */
    private String getKey(int threadId, int operationNum) {
        return "key_" + threadId + "_" + operationNum;
    }

    /**
     * Helper method to generate a consistent record ID for a given thread and
     * operation.
     * 
     * @param threadId the ID of the thread
     * @param operationNum the operation number
     * @return a consistent record ID
     */
    private long getRecord(int threadId, int operationNum) {
        return threadId * 1000 + operationNum;
    }

    /**
     * Helper method to generate a consistent value for a given thread and
     * operation.
     * 
     * @param threadId the ID of the thread
     * @param operationNum the operation number
     * @return a consistent TObject value
     */
    private TObject getValue(int threadId, int operationNum) {
        return Convert.javaToThrift("value" + threadId + "_" + operationNum);
    }

    /**
     * Reads data from the engine, targeting data written by other threads.
     *
     * @param threadId the ID of the thread
     * @param operationsCount number of operations to perform
     * @param totalThreads total number of threads in the test
     */
    private void readDataAsync(int threadId, int operationsCount,
            int totalThreads) {
        for (int i = 0; i < operationsCount; i++) {
            // Read from a different thread's writes
            int otherThread = (threadId + 1) % totalThreads;

            // Query for data that should exist
            if(i % 10 == 0 && i > 10) {
                int recordNum = i - 10; // Read slightly behind the writers
                Write write = generateWrite(otherThread, recordNum, true);

                // Test 1: Use find operation to test query functionality
                engine.find(write.getKey().toString(), Operator.EQUALS,
                        write.getValue().getTObject());

                // Test 2: Direct fetch to verify data consistency
                engine.select(write.getKey().toString(),
                        write.getRecord().longValue());
            }

            // Small sleep to allow other threads to run
            if(i % 10 == 0) {
                Threads.sleep(5);
            }
        }
    }

    /**
     * Verifies that all expected data is correctly stored in the engine.
     *
     * @param recordData map containing expected data by record
     */
    private void verifyDataConsistency(
            Map<Long, Map<String, Set<TObject>>> recordData) {
        for (Map.Entry<Long, Map<String, Set<TObject>>> recordEntry : recordData
                .entrySet()) {
            long record = recordEntry.getKey();
            Map<String, Set<TObject>> keyValues = recordEntry.getValue();

            for (Map.Entry<String, Set<TObject>> keyEntry : keyValues
                    .entrySet()) {
                String key = keyEntry.getKey();
                Set<TObject> expectedValues = keyEntry.getValue();

                // Verify each value is accessible through the engine
                for (TObject value : expectedValues) {
                    Assert.assertTrue(
                            "Data inconsistency found after concurrent operations for "
                                    + key + " at record " + record,
                            engine.verify(key, value, record));
                }

                // Verify all values are returned by select
                Set<TObject> actualValues = engine.select(key, record);
                Assert.assertTrue(
                        "Incomplete data returned for " + key + " at record "
                                + record,
                        actualValues.containsAll(expectedValues));
            }
        }
    }

    /**
     * Helper method to wait until data has been transported from buffer to
     * database.
     */
    private void waitForTransport() {
        Buffer buffer = Reflection.get("limbo", engine);
        Database database = Reflection.get("durable", engine);

        // Add data until buffer can transport
        int writeCount = 0;
        while (!Reflection.<Boolean> call(buffer, "canTransport")) {
            engine.add("wait_key_" + Time.now(),
                    Convert.javaToThrift("wait_value_" + Time.now()),
                    Time.now());
            writeCount++;
        }

        // Record when we start waiting for transport
        long startTime = Time.now();

        // Verify segments exist in database
        List<Segment> segments = Reflection.get("segments", database);
        while (segments.size() <= 1) {
            Threads.sleep(10);
            segments = Reflection.get("segments", database);
        }

        // Calculate and log transport time
        long transportTime = TimeUnit.MILLISECONDS
                .convert(Time.now() - startTime, TimeUnit.MICROSECONDS);
        if(writeCount > 0) {
            System.out.println("Transport completed: " + writeCount
                    + " writes required to force transportability");
        }
        System.out.println("Transport took " + transportTime
                + " milliseconds to complete");
        System.out.println();
    }

    /**
     * Writes data to the engine for a specific thread.
     *
     * @param threadId the ID of the thread
     * @param operationsCount number of operations to perform
     * @param expectedData map to track expected data by key
     * @param recordData map to track expected data by record
     */
    private void writeDataAsync(int threadId, int operationsCount,
            Map<String, Set<TObject>> expectedData,
            Map<Long, Map<String, Set<TObject>>> recordData) {

        for (int i = 0; i < operationsCount; i++) {
            Write write = generateWrite(threadId, i, true);

            // Add data to engine
            engine.add(write.getKey().toString(), write.getValue().getTObject(),
                    write.getRecord().longValue());

            // Track expected data by key
            Set<TObject> values = expectedData.computeIfAbsent(
                    write.getKey().toString(),
                    k -> Sets.newConcurrentHashSet());
            values.add(write.getValue().getTObject());

            // Track expected data by record
            Map<String, Set<TObject>> recordValues = recordData.computeIfAbsent(
                    write.getRecord().longValue(),
                    k -> Maps.newConcurrentMap());
            Set<TObject> keyValues = recordValues.computeIfAbsent(
                    write.getKey().toString(),
                    k -> Sets.newConcurrentHashSet());
            keyValues.add(write.getValue().getTObject());

            // Occasionally remove data to test both operations
            if(i % 5 == 0 && i > 0) {
                Write removeWrite = generateWrite(threadId, i - 1, false);

                engine.remove(removeWrite.getKey().toString(),
                        removeWrite.getValue().getTObject(),
                        removeWrite.getRecord().longValue());

                // Update expected data by key
                String removeKey = removeWrite.getKey().toString();
                TObject removeValue = removeWrite.getValue().getTObject();
                long removeRecord = removeWrite.getRecord().longValue();

                Set<TObject> removeValues = expectedData.get(removeKey);
                if(removeValues != null) {
                    removeValues.remove(removeValue);
                    if(removeValues.isEmpty()) {
                        expectedData.remove(removeKey);
                    }
                }

                // Update expected data by record
                Map<String, Set<TObject>> removeRecordValues = recordData
                        .get(removeRecord);
                if(removeRecordValues != null) {
                    Set<TObject> removeKeyValues = removeRecordValues
                            .get(removeKey);
                    if(removeKeyValues != null) {
                        removeKeyValues.remove(removeValue);
                        if(removeKeyValues.isEmpty()) {
                            removeRecordValues.remove(removeKey);
                            if(removeRecordValues.isEmpty()) {
                                recordData.remove(removeRecord);
                            }
                        }
                    }
                }
            }

            // Small sleep to allow other threads to run
            if(i % 10 == 0) {
                Threads.sleep(5);
            }
        }
    }

}
