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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.concurrent.AwaitableExecutorService;
import com.cinchapi.concourse.server.storage.db.Database;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.cinchapi.concourse.server.storage.db.kernel.Segment.Receipt;
import com.cinchapi.concourse.server.storage.temp.Buffer;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.util.Logger;

/**
 * A {@link BatchTransporter} transports a batch of multiple {@link Write
 * writes} from a {@link BatchTransportable source} and
 * {@link Database#merge(Segment, List) merges} them into a {@link Database}
 * during the same atomic operation.
 * <p>
 * Compared to the {@link StreamingTransporter}, a {@link BatchTransporter} can
 * maximize system throughput by allowing the {@link Database database} to
 * remain responsive to read and writer operations during most of the transport
 * process.
 * </p>
 *
 * @author Jeff Nelson
 */
public class BatchTransporter extends Transporter {

    /**
     * Returns a builder to configure and create a {@link BatchTransporter} with
     * the specified source.
     *
     * @param source the source from which to retrieve batches for transport
     * @return a builder for creating a {@link BatchTransporter}
     */
    public static DatabaseStage from(BatchTransportable source) {
        return new Builder().from(source);
    }

    /**
     * Return {@code true} if the {@code source} has pending patches.
     * 
     * @param source
     * @return {@code true} if the {@code source} has pending batches
     */
    private static boolean hasPendingBatches(BatchTransportable source) {
        if(source instanceof Buffer) {
            return Reflection.call(source, "canTransport");
        }
        else {
            return true;
        }
    }

    /**
     * The source from which batches are retrieved for transport.
     */
    private final BatchTransportable source;

    /**
     * The destination database where transported data is stored.
     */
    private final Database database;

    /**
     * The lock used to coordinate access during critical sections.
     */
    private final Lock lock;

    /**
     * The service used to write segments asynchronously.
     */
    private final AwaitableExecutorService segmentWriter;

    /**
     * A latch used to ensure batches are processed in the correct order.
     */
    private final CountUpLatch latch;

    /**
     * The threshold for how long a transport operation can be inactive
     * before it's considered potentially hung (in milliseconds).
     */
    private final int allowableInactivityThresholdInMillis;

    /**
     * Constructs a new {@link BatchTransporter} with the specified
     * configuration.
     * This constructor is private to enforce the use of the builder pattern.
     */
    private BatchTransporter(String threadNamePrefix,
            UncaughtExceptionHandler uncaughtExceptionHandler,
            BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier,
            int numIndexerThreads, BatchTransportable source, Database database,
            Lock lock, AwaitableExecutorService segmentWriter,
            int healthCheckFrequencyInMillis,
            int allowableInactivityThresholdInMillis) {
        super(threadNamePrefix, uncaughtExceptionHandler, executorSupplier,
                numIndexerThreads, healthCheckFrequencyInMillis);
        this.latch = new CountUpLatch();
        this.source = source;
        this.database = database;
        this.segmentWriter = segmentWriter;
        this.lock = lock;
        this.allowableInactivityThresholdInMillis = allowableInactivityThresholdInMillis;
    }

    /**
     * Processes a single batch of data from the source and transports it into
     * the database. This method ensures that batches are merged in their
     * correct chronological order, even when multiple transport threads are
     * running concurrently.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    @Override
    public void transport() throws InterruptedException {
        Batch batch = source.nextBatch();

        int batchSize = batch.size();
        int ordinal = batch.ordinal();

        // Create a Segment in the background. While the Segment is being
        // populated, the Buffer and Database remain eligible for operations.
        Segment segment = Segment.create(batchSize);
        List<Receipt> receipts = new ArrayList<>(batchSize);
        for (Write write : batch.writes()) {
            if(write != null) {
                Receipt receipt = segment.acquire(write, segmentWriter);
                receipts.add(receipt);
            }
            else {
                // The Buffer pre-populates each Page with a Write[] full of
                // nulls that are only replaced when an actual Write is added.
                // So, a null value here indicates that all the writes in the
                // Batch have been fully transported.
                break;
            }
        }

        // There may be multiple transport threads, but each Segment must be
        // appended to the Database in the same order that the corresponding
        // Page appeared in the Buffer. So each pass must queue up here until
        // its number is called
        latch.await(ordinal);
        lock.lockInterruptibly();
        try {
            // At this point, read operations in the Buffer and Database are
            // blocked. This is a short critical section where the Database
            // appends the Segment and updates its caches.
            if(database.merge(segment, receipts)) {
                source.purge(batch);

                // Signal that the next chronological transported Segment can
                // proceed to this critical section.
                latch.countUp();
            }
            else {
                throw new IllegalStateException(
                        "Unable to merge Segment while performing a batch transport");
            }
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    protected boolean requiresRestart(TransportStats stats) {
        if(stats.isTransportInProgress()) {
            // If a transport is in progress, check to see if it is taking too
            // long
            long runningTimeInMicros = stats
                    .timeSinceLastCompletedTransportStart();
            long thresholdInMicros = TimeUnit.MICROSECONDS.convert(
                    allowableInactivityThresholdInMillis,
                    TimeUnit.MILLISECONDS);
            if(runningTimeInMicros > thresholdInMicros) {
                Logger.warn(
                        "A batch transpor operation has been running for {} ms, "
                                + "which exceeds the allowable threshold of {} ms",
                        TimeUnit.MILLISECONDS.convert(runningTimeInMicros,
                                TimeUnit.MICROSECONDS),
                        allowableInactivityThresholdInMillis);
                return true;
            }
        }
        else {
            // If we're not in the middle of a transport, but there is work to
            // do, check if it's been too long since the last transport
            // completed
            long idleTimeInMicros = stats.timeSinceLastCompletedTransportEnd();
            long thresholdInMicros = TimeUnit.MICROSECONDS.convert(
                    allowableInactivityThresholdInMillis,
                    TimeUnit.MILLISECONDS);

            if(idleTimeInMicros > thresholdInMicros
                    && hasPendingBatches(source)) {
                Logger.warn(
                        "The batch transporter has been idle for {} ms with work to do, "
                                + "which exceeds the allowable threshold of {} ms",
                        TimeUnit.MILLISECONDS.convert(idleTimeInMicros,
                                TimeUnit.MICROSECONDS),
                        allowableInactivityThresholdInMillis);
                return true;
            }
        }
        return false;
    }

    /**
     * The final stage of the {@link BatchTransporter} builder which allows
     * setting optional parameters and building the transporter.
     */
    public interface BuildStage {

        /**
         * Sets the threshold for how long a transport operation can be inactive
         * before it's considered potentially hung.
         *
         * @param thresholdInMillis the threshold in milliseconds
         * @return this builder for method chaining
         */
        public BuildStage allowableInactivityThreshold(int thresholdInMillis);

        /**
         * Enables health monitoring with the specified check frequency.
         *
         * @param frequencyInMillis the frequency in milliseconds to check
         *            transport health;
         *            if not greater than 0, health monitoring is disabled
         * @return this builder for method chaining
         */
        public BuildStage healthCheck(int frequencyInMillis);

        /**
         * Builds and returns a new {@link BatchTransporter} with the configured
         * settings.
         *
         * @return a new {@link BatchTransporter}
         */
        BatchTransporter build();

        /**
         * Set the thread name format for the transporter threads, based on the
         * {@code environment}.
         * 
         * @param environment
         * @return this builder
         */
        default BuildStage environment(String environment) {
            String threadNameFormat = AnyStrings
                    .joinSimple("BatchTransporter [", environment, "]-%d");
            return threadNameFormat(threadNameFormat);
        }

        /**
         * Sets the executor service supplier for creating the thread pool.
         *
         * @param executorSupplier a function that creates the executor service
         * @return this builder
         */
        BuildStage executorSupplier(
                BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier);

        /**
         * Sets the number of threads to use for transport operations.
         *
         * @param numIndexerThreads the number of threads
         * @return this builder
         */
        BuildStage numIndexerThreads(int numIndexerThreads);

        /**
         * Sets the thread name format for the transporter threads.
         *
         * @param threadNameFormat the prefix to use for naming transporter
         *            threads
         * @return this builder
         */
        BuildStage threadNameFormat(String threadNameFormat);

        /**
         * Sets the handler for uncaught exceptions in the transporter threads.
         *
         * @param uncaughtExceptionHandler the exception handler
         * @return this builder
         */
        BuildStage uncaughtExceptionHandler(
                UncaughtExceptionHandler uncaughtExceptionHandler);
    }

    /**
     * The second stage of the {@link BatchTransporter} builder which requires
     * specifying the database.
     */
    public interface DatabaseStage {

        /**
         * Sets the destination database where transported data is stored.
         *
         * @param database the destination database
         * @return the next stage of the builder
         */
        LockStage to(Database database);
    }

    /**
     * The third stage of the {@link BatchTransporter} builder which requires
     * specifying the lock.
     */
    public interface LockStage {

        /**
         * Sets the lock used to coordinate access during critical sections.
         *
         * @param lock the lock for coordination
         * @return the next stage of the builder
         */
        SegmentWriterStage withLock(Lock lock);
    }

    /**
     * The fourth stage of the {@link BatchTransporter} builder which requires
     * specifying the segment writer.
     */
    public interface SegmentWriterStage {

        /**
         * Sets the service used to write segments asynchronously.
         *
         * @param segmentWriter the segment writer service
         * @return the final stage of the builder
         */
        BuildStage withSegmentWriter(AwaitableExecutorService segmentWriter);
    }

    /**
     * The first stage of the {@link BatchTransporter} builder which requires
     * specifying the source.
     */
    public interface SourceStage {

        /**
         * Sets the source from which to retrieve batches for transport.
         *
         * @param source the source from which to retrieve batches
         * @return the next stage of the builder
         */
        DatabaseStage from(BatchTransportable source);
    }

    /**
     * A builder for creating {@link BatchTransporter} instances with
     * customizable configuration.
     */
    private static class Builder implements
            SourceStage,
            DatabaseStage,
            LockStage,
            SegmentWriterStage,
            BuildStage {

        private BatchTransportable source;
        private Database database;
        private Lock lock;
        private AwaitableExecutorService segmentWriter;
        private String threadNamePrefix = "Batch Transporter";
        private UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
            Logger.error("Uncaught exception in batch transport thread: {}", e);
        };
        private BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier = Transporter
                .defaultExecutorSupplier();
        private int numIndexerThreads = 1;
        private int healthCheckFrequencyInMillis = 10000;
        private int allowableInactivityThresholdInMillis = 5000;

        @Override
        public BuildStage allowableInactivityThreshold(int thresholdInMillis) {
            this.allowableInactivityThresholdInMillis = thresholdInMillis;
            return this;
        }

        @Override
        public BatchTransporter build() {
            return new BatchTransporter(threadNamePrefix,
                    uncaughtExceptionHandler, executorSupplier,
                    numIndexerThreads, source, database, lock, segmentWriter,
                    healthCheckFrequencyInMillis,
                    allowableInactivityThresholdInMillis);
        }

        @Override
        public BuildStage executorSupplier(
                BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier) {
            this.executorSupplier = executorSupplier;
            return this;
        }

        @Override
        public DatabaseStage from(BatchTransportable source) {
            this.source = source;
            return this;
        }

        @Override
        public BuildStage healthCheck(int frequencyInMillis) {
            this.healthCheckFrequencyInMillis = frequencyInMillis;
            return this;
        }

        @Override
        public BuildStage numIndexerThreads(int numIndexerThreads) {
            this.numIndexerThreads = numIndexerThreads;
            return this;
        }

        @Override
        public BuildStage threadNameFormat(String threadNameFormat) {
            this.threadNamePrefix = threadNameFormat;
            return this;
        }

        @Override
        public LockStage to(Database database) {
            this.database = database;
            return this;
        }

        @Override
        public BuildStage uncaughtExceptionHandler(
                UncaughtExceptionHandler uncaughtExceptionHandler) {
            this.uncaughtExceptionHandler = uncaughtExceptionHandler;
            return this;
        }

        @Override
        public SegmentWriterStage withLock(Lock lock) {
            this.lock = lock;
            return this;
        }

        @Override
        public BuildStage withSegmentWriter(
                AwaitableExecutorService segmentWriter) {
            this.segmentWriter = segmentWriter;
            return this;
        }
    }
}
