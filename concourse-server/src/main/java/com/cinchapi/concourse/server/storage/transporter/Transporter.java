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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import com.cinchapi.common.base.CheckedExceptions;
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
     * @param threadNamePrefix the prefix to use for naming transport threads
     * @param uncaughtExceptionHandler handler for uncaught exceptions in
     *            transport threads
     */
    Transporter(String threadNamePrefix,
            UncaughtExceptionHandler uncaughtExceptionHandler) {
        this(threadNamePrefix, uncaughtExceptionHandler,
                defaultExecutorSupplier(), 1);
    }

    /**
     * Constructs a {@link Transporter} with customizable thread pool and thread
     * count.
     *
     * @param threadNamePrefix the prefix to use for naming transport threads
     * @param uncaughtExceptionHandler handler for uncaught exceptions in
     *            transport threads
     * @param executorSupplier a function that creates the
     *            {@link ExecutorService} to use
     * @param numIndexerThreads the number of threads to use for transport
     *            operations
     */
    Transporter(String threadNamePrefix,
            UncaughtExceptionHandler uncaughtExceptionHandler,
            BiFunction<String, UncaughtExceptionHandler, ExecutorService> executorSupplier,
            int numIndexerThreads) {
        this.threadNameFormat = threadNamePrefix;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.executorSupplier = executorSupplier;
        this.numIndexerThreads = numIndexerThreads;
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
        }
        else {
            throw new IllegalStateException(
                    "The Transporter is already running");
        }
    }

    /**
     * Stops the transport process by shutting down the executor service and
     * interrupting any running transport threads. If the {@link Transporter} is
     * not
     * running, an {@link IllegalStateException} is thrown.
     *
     * @throws IllegalStateException if the {@link Transporter} is not running
     */
    public void stop() {
        if(running.compareAndSet(true, false)) {
            executor.shutdownNow();
            executor = null;
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
     * <p>
     * If a fatal error occurred or the processes appear stalled, this method
     * can be used to try to reset and try again.
     * <p>
     */
    protected void restart() {
        running.set(false);
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
        running.set(true);
        tasks = submitTasks();
    }

    // TODO: bring hang detection to this class?

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
                        transport();
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
}
