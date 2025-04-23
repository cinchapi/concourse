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
package com.cinchapi.concourse.ete.performance;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit test for the throughput of the different transporters
 *
 * @author Jeff Nelson
 */
public abstract class AbstractTransporterThroughputTest
        extends ClientServerTest {

    /**
     * Count the number of files with a specific extension in a directory.
     * 
     * @param directory the directory to search
     * @param extension the file extension to count
     * @return the count of files with the specified extension
     */
    private static long countFilesWithExtension(Path directory,
            String extension) {
        try {
            if(!java.nio.file.Files.exists(directory)) {
                return 0;
            }

            return java.nio.file.Files.list(directory)
                    .filter(path -> path.toString().endsWith(extension))
                    .count();
        }
        catch (Exception e) {
            System.err.println("Error counting files in " + directory + ": "
                    + e.getMessage());
            return 0;
        }
    }

    /**
     * Generate a deterministic string of the specified length.
     * 
     * @param length the target length of the string
     * @param seed a seed value to make the generation deterministic but varied
     * @return a generated string
     */
    protected static String generateString(int length, long seed) {
        StringBuilder sb = new StringBuilder();

        // Add some searchable terms that will be consistent
        String[] searchTerms = { "database", "concourse", "performance",
                "throughput", "testing", "indexing", "search", "query",
                "engine", "buffer" };

        // Add a search term based on the seed
        sb.append(searchTerms[(int) (seed % searchTerms.length)]).append(" ");

        // Fill the rest with deterministic content
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        while (sb.length() < length) {
            // Use the seed and current length to deterministically select
            // characters
            int charIndex = (int) ((seed + sb.length()) % chars.length());
            sb.append(chars.charAt(charIndex));

            // Occasionally add spaces and one of the search terms
            if(sb.length() % 10 == 0) {
                sb.append(" ");
                if(sb.length() % 50 == 0) {
                    int termIndex = (int) ((seed + sb.length())
                            % searchTerms.length);
                    sb.append(searchTerms[termIndex]).append(" ");
                }
            }
        }

        // Trim to exact length
        return sb.substring(0, length);
    }

    /**
     * Start the reader threads that will concurrently read from the engine.
     * 
     * @param client the client to read from
     * @param stopReading atomic flag to signal when to stop reading
     * @param readCount counter for completed read operations
     * @param startLatch latch to synchronize the start of all threads
     * @return list of started threads
     */
    protected static List<Thread> startReaderThreads(Concourse concourse,
            AtomicBoolean stopReading, AtomicLong readCount,
            CountDownLatch startLatch) {
        ConnectionPool pool = ConnectionPool.newCachedConnectionPool(concourse);
        List<Thread> threads = new ArrayList<>();

        // 3 threads doing select on key/value
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                Concourse client = pool.request();
                try {
                    // Wait for signal to start
                    startLatch.await();

                    while (!stopReading.get()) {
                        // Select different keys for each thread to avoid
                        // contention
                        String key = "name";
                        long record = (threadId * 10 + 1) % NUM_RECORDS + 1;

                        client.select(key, record);
                        readCount.incrementAndGet();

                        // Sleep for 50ms to simulate processing time
                        Thread.sleep(50);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    pool.release(client);
                }
            });
            thread.setName("KeyValueSelectThread-" + i);
            thread.start();
            threads.add(thread);
        }

        // 3 threads doing select on record
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                Concourse client = pool.request();
                try {
                    // Wait for signal to start
                    startLatch.await();

                    while (!stopReading.get()) {
                        // Use different records for each thread
                        long record = (threadId * 15 + 5) % NUM_RECORDS + 1;

                        client.select(record);
                        readCount.incrementAndGet();

                        // Sleep for 50ms to simulate processing time
                        Thread.sleep(50);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    pool.release(client);
                }
            });
            thread.setName("RecordSelectThread-" + i);
            thread.start();
            threads.add(thread);
        }

        // 1 thread doing search
        Thread searchThread = new Thread(() -> {
            Concourse client = pool.request();
            try {
                // Wait for signal to start
                startLatch.await();

                int searchTermIndex = 0;
                String[] searchTerms = { "database", "concourse", "performance",
                        "throughput", "testing", "indexing", "search", "query",
                        "engine", "buffer" };

                while (!stopReading.get()) {
                    String key = "description";
                    String query = searchTerms[searchTermIndex];
                    searchTermIndex = (searchTermIndex + 1)
                            % searchTerms.length;

                    client.search(key, query);
                    readCount.incrementAndGet();

                    // Sleep for 50ms to simulate processing time
                    Thread.sleep(50);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                pool.release(client);
            }
        });
        searchThread.setName("SearchThread");
        searchThread.start();
        threads.add(searchThread);

        // 2 threads doing find
        for (int i = 0; i < 2; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                Concourse client = pool.request();
                try {
                    // Wait for signal to start
                    startLatch.await();

                    while (!stopReading.get()) {
                        // Alternate between different keys and values
                        String key = threadId % 2 == 0 ? "age" : "salary";
                        Object value = threadId % 2 == 0
                                ? (20 + (threadId * 5) % 50)
                                : // age value
                                (50000.0 + (threadId * 10000.0)); // salary
                                                                  // value

                        client.find(key, Operator.EQUALS, value);
                        readCount.incrementAndGet();

                        // Sleep for 50ms to simulate processing time
                        Thread.sleep(50);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    pool.release(client);
                }
            });
            thread.setName("FindThread-" + i);
            thread.start();
            threads.add(thread);
        }

        // 1 thread doing find with range query
        Thread rangeThread = new Thread(() -> {
            Concourse client = pool.request();
            try {
                // Wait for signal to start
                startLatch.await();

                int rangeStart = 0;
                while (!stopReading.get()) {
                    String key = "score";
                    int min = rangeStart % 50;
                    int max = min + 20;
                    rangeStart = (rangeStart + 7) % 50; // Use a prime number to
                                                        // cycle through
                                                        // different ranges

                    client.find(key, Operator.BETWEEN, min, max);
                    readCount.incrementAndGet();

                    // Sleep for 50ms to simulate processing time
                    Thread.sleep(50);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                pool.release(client);
            }
        });
        rangeThread.setName("RangeFindThread");
        rangeThread.start();
        threads.add(rangeThread);

        // 1 thread doing verify
        Thread verifyThread = new Thread(() -> {
            Concourse client = pool.request();
            try {
                // Wait for signal to start
                startLatch.await();

                int recordIndex = 0;
                while (!stopReading.get()) {
                    String key = "active";
                    boolean value = recordIndex % 2 == 0;
                    long record = recordIndex % NUM_RECORDS + 1;
                    recordIndex = (recordIndex + 1) % NUM_RECORDS;

                    client.verify(key, value, record);
                    readCount.incrementAndGet();

                    // Sleep for 50ms to simulate processing time
                    Thread.sleep(50);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                pool.release(client);
            }
        });
        verifyThread.setName("VerifyThread");
        verifyThread.start();
        threads.add(verifyThread);

        return threads;
    }

    /**
     * Add a variety of data types to the engine in a predictable pattern.
     * 
     * @param client the client to populate with data
     * @return the total number of writes added to the engine
     */
    protected static long writeData(Concourse client) {
        // Define a set of keys to use
        String[] keys = { "name", "age", "email", "address", "phone", "salary",
                "active", "score", "rating", "description" };

        long totalWrites = 0;

        // Add data for each record
        for (long record = 1; record <= NUM_RECORDS; record++) {
            // Add a boolean value
            client.add("active", record % 2 == 0, record);
            totalWrites++;

            // Add integer values
            client.add("age", 20 + (record % 50), record);
            totalWrites++;
            client.add("score", record % 100, record);
            totalWrites++;

            // Add long values
            client.add("id", record * 1000, record);
            totalWrites++;
            client.add("timestamp", System.currentTimeMillis() - record,
                    record);
            totalWrites++;

            // Add double values
            client.add("salary", 50000.0 + (record % 100) * 1000.0, record);
            totalWrites++;
            client.add("rating", 1.0 + (record % 5), record);
            totalWrites++;

            // Add small strings
            String smallString = generateString(STRING_LENGTH_SMALL, record);
            client.add("name", smallString, record);
            totalWrites++;
            client.add("email", "user" + record + "@example.com", record);
            totalWrites++;

            // Add medium strings
            String mediumString = generateString(STRING_LENGTH_MEDIUM, record);
            client.add("address", mediumString, record);
            totalWrites++;
            client.add("bio", mediumString, record);
            totalWrites++;

            // Add large strings
            String largeString = generateString(STRING_LENGTH_LARGE, record);
            client.add("description", largeString, record);
            totalWrites++;
            client.add("content", largeString, record);
            totalWrites++;

            // Add tags
            client.add("tags", "#tag" + (record % 10), record);
            totalWrites++;
            client.add("tags", "#common", record);
            totalWrites++;

            // Add additional values to create a variety of data per record
            for (int i = 0; i < VALUES_PER_RECORD; i++) {
                String key = keys[i % keys.length];

                // Determine the type of value to add based on a predictable
                // pattern
                Object value;
                int type = i % 5;

                switch (type) {
                case 0: // Boolean
                    value = (i % 2 == 0);
                    break;
                case 1: // Integer
                    value = i * 10 + (int) record;
                    break;
                case 2: // Double
                    value = i * 10.5 + record / 10.0;
                    break;
                case 3: // Small String
                    value = "Value-" + i + "-" + record;
                    break;
                case 4: // Medium String
                    value = generateString(STRING_LENGTH_SMALL + i, record + i);
                    break;
                default:
                    value = "default";
                }

                client.add(key, value, record);
                totalWrites++;
            }

            // Add some removals to test that operation as well
            if(record > 1 && record % 10 == 0) {
                // Fix: Only remove values that were actually added to the
                // previous record
                client.remove("active", (record - 1) % 2 == 0, record - 1);
                totalWrites++;
                client.remove("tags", "#tag" + ((record - 1) % 10), record - 1);
                totalWrites++;
            }
        }

        return totalWrites;
    }

    static final int NUM_RECORDS = 250;
    static final int VALUES_PER_RECORD = 10;

    static final int STRING_LENGTH_SMALL = 100;

    static final int STRING_LENGTH_MEDIUM = 1000;

    static final int STRING_LENGTH_LARGE = 5000;

    static final int TEST_DURATION_SECONDS = 10;

    @Test
    public void testThroughput() throws InterruptedException {
        System.out.println("Writing data....");
        long writeCount = writeData(client);

        // Start concurrent reader threads
        AtomicBoolean stopReading = new AtomicBoolean(false);
        AtomicLong readCount = new AtomicLong(0);

        // Create latch for synchronizing thread start
        CountDownLatch startLatch = new CountDownLatch(1);

        // Start reader threads
        startReaderThreads(client, stopReading, readCount, startLatch);

        // Record start time
        long startTime = System.currentTimeMillis();

        // Signal all threads to start
        startLatch.countDown();

        // Let the test run for the specified duration
        System.out.println(AnyStrings.format("Reading data for {} seconds....",
                TEST_DURATION_SECONDS));
        Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_DURATION_SECONDS));

        // Signal threads to stop
        stopReading.set(true);

        // Record end time and calculate elapsed time
        long endTime = System.currentTimeMillis();
        long elapsedTimeMs = endTime - startTime;

        // Count the number of segment and buffer files
        Path databaseDir = server.getDatabaseDirectory();
        Path bufferDir = server.getBufferDirectory();
        System.out.println(bufferDir);

        long segmentCount = countFilesWithExtension(databaseDir
                .resolve(client.getServerEnvironment()).resolve("segments"),
                ".seg");
        long blockCount = countFilesWithExtension(
                bufferDir.resolve(client.getServerEnvironment()), ".buf");

        // Print results
        System.out.println("Test Results:");
        System.out.println("-------------");
        System.out.println(
                "Elapsed time: " + String.format("%,d", elapsedTimeMs) + " ms");
        System.out.println("Total read operations: "
                + String.format("%,d", readCount.get()));
        System.out.println("Read operations per second: " + String
                .format("%,.2f", readCount.get() / (elapsedTimeMs / 1000.0)));
        System.out.println("Segment files (indexed data): "
                + String.format("%,d", segmentCount));
        System.out.println("Buffer files (pending data): "
                + String.format("%,d", blockCount));
        System.out.println("Total writes: " + String.format("%,d", writeCount));
        System.out.println("Indexing ratio: " + String.format("%,.2f%%",
                (segmentCount * 100.0) / (segmentCount + blockCount)));
    }

    @Override
    protected void beforeEachTest() {
        String transporter = enableBatchTransporter() ? "batch" : "streaming";
        server.config().set("transporter", transporter);
        server.restart();
        client = server.connect();
    }

    protected abstract boolean enableBatchTransporter();

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}
