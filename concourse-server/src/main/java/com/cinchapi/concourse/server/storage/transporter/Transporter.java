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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link Transporter} provides a framework for asynchronous transport
 * operations
 * where data is pulled from a {@link Store source} and transported to a
 * {@link Store destination}. It manages background threads that
 * continuously process transport tasks as defined by subclasses.
 * <p>
 * The {@link Transporter} is designed to be agnostic about the specific
 * transport technique (streaming, batch processing, etc.) and the particular
 * data {@link Store stores} involved. It focuses solely on managing the
 * lifecycle of transport threads, including starting, stopping, and handling
 * thread interruptions.
 * </p>
 * <p>
 * Subclasses must implement the {@link #transport()} method to define the
 * specific transport logic that should be executed during each pass of the
 * background thread. They should also provide mechanisms for users to specify
 * the particular data sources and destinations.
 * </p>
 * <p>
 * Using a {@link Transporter} encapsulates the runtime processing of transport
 * operations and creates a pluggable framework for different transport
 * techniques
 * while handling common concerns like thread management and error handling.
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class Transporter {

    /**
     * Returns the default executor supplier that creates a single-threaded
     * executor with the specified thread name prefix and exception handler.
     *
     * @return a function that creates an appropriate {@link ExecutorService}
     */
    static BiFunction<String, UncaughtExceptionHandler, ExecutorService> defaultExecutorSupplier() {
        return (threadNameFormat, uncaughtExceptionHandler) -> {
            ThreadFactory factory = new ThreadFactoryBuilder()
                    .setNameFormat(threadNameFormat).setDaemon(true)
                    .setUncaughtExceptionHandler(uncaughtExceptionHandler)
                    .build();
            return Executors.newSingleThreadExecutor(factory);
        };
    }

    /**
     * Timer for scheduling health checks.
     */
    private Timer healthCheckTimer;

    /**
     * The frequency with which to check transport health, in milliseconds.
     * A value of 0 or less disables health monitoring.
     */
    protected final int healthCheckFrequencyInMillis; // visible for testing

    /**
     * Statistics about transport operations.
     */
    private final TransportStats stats = new TransportStats();

    /**
     * A function that creates an {@link ExecutorService} with the specified
     * thread name prefix and exception handler.
     */
    private final BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier;

    /**
     * The {@link ExecutorService} that manages the background transport
     * threads.
     */
    private ExecutorService executor;

    /**
     * Flag indicating whether the {@link Transporter} is currently running.
     * This is used for thread coordination and safe shutdown.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * The number of threads to use for transport operations.
     */
    private final int numIndexerThreads;

    /**
     * The prefix to use for naming transport threads, which helps with
     * debugging and monitoring.
     */
    private final String threadNameFormat;

    /**
     * Handler for uncaught exceptions that occur in the transport threads.
     */
    private final UncaughtExceptionHandler uncaughtExceptionHandler;

    /**
     * The collection of active transport tasks.
     */
    private List<Future<?>> tasks;

    /**
     * Constructs a {@link Transporter} with a single worker thread.
     *
     * @param threadNameFormat the form to use for naming transport threads
     * @param uncaughtExceptionHandler handler for uncaught exceptions in
     *            transport threads
     */
    Transporter(String threadNameFormat,
            UncaughtExceptionHandler uncaughtExceptionHandler) {
        this(threadNameFormat, uncaughtExceptionHandler,
                defaultExecutorSupplier(), 1, 0);
    }

    /**
     * Constructs a {@link Transporter} with customizable thread pool and thread
     * count.
     *
     * @param threadNameFormat the format to use for naming transport threads
     * @param uncaughtExceptionHandler handler for uncaught exceptions in
     *            transport threads
     * @param executorSupplier a function that creates the
     *            {@link ExecutorService} to use
     * @param numIndexerThreads the number of threads to use for transport
     *            operations
     * @param healthCheckFrequencyInMillis the frequency in milliseconds to
     *            check transport health;
     *            if not greater than 0, health monitoring is disabled
     */
    Transporter(String threadNameFormat,
            UncaughtExceptionHandler uncaughtExceptionHandler,
            BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier,
            int numIndexerThreads, int healthCheckFrequencyInMillis) {
        this.threadNameFormat = threadNameFormat;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.executorSupplier = executorSupplier;
        this.numIndexerThreads = numIndexerThreads;
        this.healthCheckFrequencyInMillis = healthCheckFrequencyInMillis;
    }

    /**
     * Constructs a {@link Transporter} with a single worker thread and optional
     * health monitoring.
     *
     * @param threadNameFormat the form to use for naming transport threads
     * @param uncaughtExceptionHandler handler for uncaught exceptions in
     *            transport threads
     * @param healthCheckFrequencyInMillis the frequency in milliseconds to
     *            check transport health;
     *            if not greater than 0, health monitoring is disabled
     */
    Transporter(String threadNameFormat,
            UncaughtExceptionHandler uncaughtExceptionHandler,
            int healthCheckFrequencyInMillis) {
        this(threadNameFormat, uncaughtExceptionHandler,
                defaultExecutorSupplier(), 1, healthCheckFrequencyInMillis);
    }

    /**
     * Starts the transport process by launching background threads that
     * repeatedly call the {@link #transport()} method. If the
     * {@link Transporter} is
     * already running, an {@link IllegalStateException} is thrown.
     *
     * @throws IllegalStateException if the {@link Transporter} is already
     *             running
     */
    public void start() {
        if(running.compareAndSet(false, true)) {
            executor = executorSupplier.apply(threadNameFormat,
                    uncaughtExceptionHandler);
            tasks = submitTasks();

            if(healthCheckFrequencyInMillis > 0) {
                startHealthMonitoring();
            }
        }
        else {
            throw new IllegalStateException(
                    "The Transporter is already running");
        }
    }

    /**
     * Returns the current transport statistics.
     * 
     * @return the current transport statistics
     */
    public final TransportStats stats() {
        return stats;
    }

    /**
     * Stop the {@link Transporter} and make a <em>best effort</em> to interrupt
     * any transports that are currently in progress.
     * 
     * <p>
     * When this method returns, the {@link Transporter} will be marked as
     * stopped and no new transports will be scheduled, but it is possible that
     * prior initiated transports will still complete. If it is necessary to
     * guarantee that all pending transports are cancelled or completed before
     * returning, use the {@link #stop(boolean) stop(true)} instead.
     * </p>
     *
     * @throws IllegalStateException if the {@link Transporter} is not running
     */
    public void stop() {
        if(running.compareAndSet(true, false)) {
            executor.shutdownNow();
            executor = null;

            if(healthCheckTimer != null) {
                healthCheckTimer.cancel();
                healthCheckTimer = null;
            }
        }
        else {
            throw new IllegalStateException("The Transporter is not running");
        }
    }

    /**
     * Stop the {@link Transporter} and optionally {@code wait} for all active
     * transports to finish.
     * 
     * <p>
     * In general, it is best to use the {@link #stop()} method, but this one is
     * beneficial for unit tests that want to preserve state and guarantee that
     * there are no lingering transport processes modifying things in the
     * background.
     * </p>
     * 
     * @param wait
     */
    public void stop(boolean wait) {
        if(running.compareAndSet(true, false)) {
            executor.shutdown();
            if(wait) {
                try {
                    executor.awaitTermination(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException e) {}
            }
            executor = null;
            if(healthCheckTimer != null) {
                healthCheckTimer.cancel();
                healthCheckTimer = null;
            }
        }
        else {
            throw new IllegalStateException("The Transporter is not running");
        }
    }

    /**
     * Defines the transport operation to be performed in each cycle.
     * Subclasses must implement this method to specify the logic for
     * transferring data from the source to the destination. This method is
     * called repeatedly by the background thread as long as the
     * {@link Transporter} is running.
     *
     * @throws InterruptedException if the thread is interrupted while
     *             performing the transport operation
     */
    public abstract void transport() throws InterruptedException;

    /**
     * Determines whether the transport tasks require a restart based on
     * current health metrics. Subclasses should override this method to
     * implement their specific health check logic.
     * 
     * @param stats the current transport statistics
     * @return true if the transport tasks should be restarted
     */
    protected abstract boolean requiresRestart(TransportStats stats);

    /**
     * <p>
     * If a fatal error occurred or the processes appear stalled, this method
     * can be used to try to reset and try again.
     * <p>
     */
    protected void restart() {
        if(running.compareAndSet(true, false)) {
            for (Future<?> task : tasks) {
                if(task.isDone()) {
                    try {
                        // Attempt to bubble up any errors that happened
                        task.get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        Logger.error("A Transporter error occured", e);
                    }
                }
                else {
                    task.cancel(true);
                }

            }
            if(running.compareAndSet(false, true)) {
                tasks = submitTasks();
            }
        }
    }

    /**
     * Starts the health monitoring timer that periodically checks if the
     * transport tasks are functioning properly.
     */
    private void startHealthMonitoring() {
        healthCheckTimer = new Timer(true);
        healthCheckTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if(requiresRestart(stats)) {
                    Logger.warn(
                            "Transport health check indicates a restart is needed. "
                                    + "Attempting to restart transport tasks.");
                    restart();
                }
            }
        }, healthCheckFrequencyInMillis, healthCheckFrequencyInMillis);
    }

    /**
     * Creates and submits transport tasks to the executor.
     * 
     * @return the list of submitted tasks
     */
    private List<Future<?>> submitTasks() {
        List<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < numIndexerThreads; ++i) {
            Future<?> task = executor.submit(() -> {
                while (running.get()) {
                    try {
                        stats.recordTransportStart();
                        transport();
                        stats.recordTransportEnd();
                    }
                    catch (InterruptedException e) {
                        // This usually indicates that the Transporter has
                        // been stopped, so re-spin to confirm before
                        // exiting
                        Thread.currentThread().interrupt();
                        continue;
                    }
                    catch (Exception e) {
                        uncaughtExceptionHandler
                                .uncaughtException(Thread.currentThread(), e);

                        // Re-throw the exception so that the task is marked as
                        // failed and triggers a potential restart
                        throw CheckedExceptions.throwAsRuntimeException(e);
                    }
                }
            });
            tasks.add(task);
        }
        return tasks;
    }

    /**
     * A lightweight class that tracks statistics about transport operations.
     * This class maintains state for each thread and calculates metrics on
     * demand.
     */
    protected static class TransportStats {

        /**
         * Registry of transport threads with their operation timestamps.
         */
        private final ConcurrentHashMap<Long, Timing> timing = new ConcurrentHashMap<>();

        /**
         * Running average of transport operation durations in microseconds.
         */
        private final AtomicLong avgTransportDuration = new AtomicLong(0);

        /**
         * Count of transport operations completed.
         */
        private final AtomicLong numCompletedTransports = new AtomicLong(0);

        /**
         * Returns the running average of transport operation durations.
         * 
         * @return the average transport duration in microseconds
         */
        public long avgTransportDuration() {
            return avgTransportDuration.get();
        }

        /**
         * Returns true if a transport operation is currently in progress for
         * any thread.
         * 
         * @return true if a transport is in progress
         */
        public boolean isTransportInProgress() {
            return timing.values().stream()
                    .anyMatch(ts -> ts.currentTransportStart > 0);
        }

        /**
         * Returns the duration of the last completed transport operation.
         * 
         * @return the last completed transport duration in microseconds, or 0
         *         if no transport has completed
         */
        public long lastCompletedTransportDuration() {
            Timing latest = getMostRecentlyCompletedThreadStats();
            return latest != null ? latest.lastCompletedDuration() : 0;
        }

        /**
         * Returns the timestamp when the last transport operation completed.
         * 
         * @return the last transport end time in microseconds, or 0 if no
         *         transport has completed
         */
        public long lastCompletedTransportEndTime() {
            Timing latest = getMostRecentlyCompletedThreadStats();
            return latest != null ? latest.lastCompletedTransportEnd : 0;
        }

        /**
         * Returns the timestamp when the last completed transport operation
         * started.
         * 
         * @return the last completed transport start time in microseconds, or 0
         *         if no transport has completed
         */
        public long lastCompletedTransportStartTime() {
            Timing latest = getMostRecentlyCompletedThreadStats();
            return latest != null ? latest.lastCompletedTransportStart : 0;
        }

        /**
         * Returns the number of transport operations completed.
         * 
         * @return the transport count
         */
        public long numCompletedTransports() {
            return numCompletedTransports.get();
        }

        /**
         * Returns the time elapsed since the last transport operation
         * completed, in microseconds.
         * 
         * @return elapsed time since last transport completion, or 0 if no
         *         transport
         *         has completed
         */
        public long timeSinceLastCompletedTransportEnd() {
            long endTime = lastCompletedTransportEndTime();
            return endTime > 0 ? Time.now() - endTime : 0;
        }

        /**
         * Returns the time elapsed since the last transport operation started,
         * in microseconds.
         * 
         * @return elapsed time since last transport start, or 0 if no transport
         *         has started
         */
        public long timeSinceLastCompletedTransportStart() {
            long startTime = lastCompletedTransportStartTime();
            return startTime > 0 ? Time.now() - startTime : 0;
        }

        /**
         * Returns a string representation of the transport statistics formatted
         * as a table.
         * 
         * @return a string containing the current transport statistics in
         *         tabular format
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Transport Statistics:\n");
            sb.append(
                    "+------------------------------------------+----------------------+\n");
            sb.append(String.format("| %-40s | %-20s |\n", "Metric", "Value"));
            sb.append(
                    "+------------------------------------------+----------------------+\n");
            sb.append(String.format("| %-40s | %-20d |\n",
                    "Completed Transports", numCompletedTransports()));
            sb.append(String.format("| %-40s | %-20.2f |\n",
                    "Average Transport Duration (ms)",
                    avgTransportDuration() / 1000.0));

            long duration = lastCompletedTransportDuration();
            String durationStr = duration > 0
                    ? String.format("%.2f", duration / 1000.0)
                    : "-";
            sb.append(String.format("| %-40s | %-20s |\n",
                    "Last Completed Transport Duration (ms)", durationStr));

            sb.append(String.format("| %-40s | %-20s |\n",
                    "Transport In Progress", isTransportInProgress()));

            long startTime = lastCompletedTransportStartTime();
            String startTimeStr = startTime > 0 ? String.valueOf(startTime)
                    : "-";
            sb.append(String.format("| %-40s | %-20s |\n",
                    "Last Completed Transport Start Time", startTimeStr));

            long endTime = lastCompletedTransportEndTime();
            String endTimeStr = endTime > 0 ? String.valueOf(endTime) : "-";
            sb.append(String.format("| %-40s | %-20s |\n",
                    "Last Completed Transport End Time", endTimeStr));

            long timeSinceEnd = timeSinceLastCompletedTransportEnd();
            String timeSinceEndStr = timeSinceEnd > 0
                    ? String.format("%.2f", timeSinceEnd / 1000.0)
                    : "-";
            sb.append(String.format("| %-40s | %-20s |\n",
                    "Time Since Last Transport End (ms)", timeSinceEndStr));

            sb.append(
                    "+------------------------------------------+----------------------+");
            return sb.toString();
        }

        /**
         * Records the end of a transport operation and updates statistics.
         */
        void recordTransportEnd() {
            long threadId = Thread.currentThread().getId();
            Timing stats = timing.computeIfAbsent(threadId, id -> new Timing());

            // Only update stats if this was a valid transport operation
            if(stats.currentTransportStart > 0) {
                long now = Time.now();
                stats.lastCompletedTransportEnd = now;
                stats.lastCompletedTransportStart = stats.currentTransportStart;
                stats.currentTransportStart = 0; // Reset to indicate no
                                                 // in-progress operation

                long duration = stats.lastCompletedDuration();

                // Update average duration
                long count = numCompletedTransports.incrementAndGet();
                if(count == 1) {
                    avgTransportDuration.set(duration);
                }
                else {
                    long oldAvg = avgTransportDuration.get();
                    long newAvg = oldAvg + (duration - oldAvg) / count;
                    avgTransportDuration.set(newAvg);
                }
            }
        }

        /**
         * Records the start of a transport operation.
         */
        void recordTransportStart() {
            long threadId = Thread.currentThread().getId();
            Timing stats = timing.computeIfAbsent(threadId, id -> new Timing());
            stats.currentTransportStart = Time.now();
        }

        /**
         * Gets the thread stats with the most recent completed transport.
         * 
         * @return the thread stats with the most recent end time, or null if
         *         none
         */
        private Timing getMostRecentlyCompletedThreadStats() {
            return timing.values().stream()
                    .filter(ts -> ts.lastCompletedTransportEnd > 0)
                    .max((a, b) -> Long.compare(a.lastCompletedTransportEnd,
                            b.lastCompletedTransportEnd))
                    .orElse(null);
        }

        /**
         * Container for thread-specific transport timing.
         */
        private static class Timing {

            /**
             * The timestamp when the thread's current transport operation
             * started.
             * Zero if no transport is in progress.
             */
            long currentTransportStart;

            /**
             * The timestamp when the thread's last completed transport
             * operation started.
             * Zero if no transport has completed.
             */
            long lastCompletedTransportStart;

            /**
             * The timestamp when the thread's last transport operation
             * completed.
             * Zero if no transport has completed.
             */
            long lastCompletedTransportEnd;

            /**
             * Creates a new time container with no recorded operations.
             */
            Timing() {
                this.currentTransportStart = 0;
                this.lastCompletedTransportStart = 0;
                this.lastCompletedTransportEnd = 0;
            }

            /**
             * Calculates the duration of the last completed transport
             * operation.
             * 
             * @return the duration in microseconds, or 0 if no transport has
             *         completed
             */
            long lastCompletedDuration() {
                return (lastCompletedTransportStart > 0
                        && lastCompletedTransportEnd > 0)
                                ? lastCompletedTransportEnd
                                        - lastCompletedTransportStart
                                : 0;
            }
        }
    }
}
