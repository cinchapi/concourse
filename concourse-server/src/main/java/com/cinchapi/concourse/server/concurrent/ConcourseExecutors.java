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
package com.cinchapi.concourse.server.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.cinchapi.concourse.annotate.UtilityClass;
import com.cinchapi.concourse.util.Logger;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A collection of thread and concurrency related utility methods.
 * 
 * @author Jeff Nelson
 */
@UtilityClass
public final class ConcourseExecutors {

    /**
     * Create an {@link ExecutorService} thread pool with enough threads to
     * execute {@code commands} and block until all the tasks have completed.
     * 
     * @param threadNamePrefix
     * @param commands
     */
    public static void executeAndAwaitTermination(String threadNamePrefix,
            Runnable... commands) {
        BlockingExecutorService executor = executors.get(threadNamePrefix);
        if(executor == null) {
            executor = BlockingExecutorService.create(threadNamePrefix);
            executors.put(threadNamePrefix, executor);
        }
        executor.execute(commands);
    }

    /**
     * Create a temporary {@link ExecutorService} thread pool with enough
     * threads to execute {@code commands} and block until all the tasks have
     * completed. Afterwards, the thread pool is shutdown.
     * 
     * @param threadNamePrefix
     * @param commands
     */
    public static void executeAndAwaitTerminationAndShutdown(
            String threadNamePrefix, Runnable... commands) {
        ExecutorService executor = Executors.newFixedThreadPool(
                commands.length, getThreadFactory(threadNamePrefix));
        for (Runnable command : commands) {
            executor.execute(command);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    public static ExecutorService newCachedThreadPool(String threadNamePrefix) {
        return Executors
                .newCachedThreadPool(getThreadFactory(threadNamePrefix));
    }

    /**
     * Return a {@link ExecutorService} thread pool with {@code num} threads,
     * each whose name is prefixed with {@code threadNamePrefix}.
     * 
     * @param num
     * @param threadNamePrefix
     * @return a new thread pool
     */
    public static ExecutorService newThreadPool(int num, String threadNamePrefix) {
        return Executors.newFixedThreadPool(num,
                getThreadFactory(threadNamePrefix));
    }

    /**
     * Return a thread factory that will name all threads using
     * {@code threadNamePrefix}.
     * 
     * @param threadNamePrefix
     * @return the thread factory
     */
    private static ThreadFactory getThreadFactory(String threadNamePrefix) {
        return new ThreadFactoryBuilder()
                .setNameFormat(threadNamePrefix + " #%d")
                .setUncaughtExceptionHandler(uncaughtExceptionHandler).build();
    }

    /**
     * A cache of ExecutorServices that are associated with a given
     * threadNamePrefix.
     */
    private static final Map<String, BlockingExecutorService> executors = Maps
            .newHashMap();

    /**
     * Catches exceptions thrown from pooled threads. For the Database,
     * exceptions will occur in the event that an attempt is made to write a
     * duplicate non-offset write when the system shuts down in the middle of a
     * buffer flush. Those exceptions can be ignored, so we catch them here and
     * print log statements.
     */
    private static final UncaughtExceptionHandler uncaughtExceptionHandler;

    static {
        uncaughtExceptionHandler = new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                Logger.warn("Uncaught exception in thread '{}'. This possibly "
                        + "indicates that the system shutdown prematurely "
                        + "during a buffer transport operation.", t);
                Logger.warn("", e);

            }

        };
    }

    private ConcourseExecutors() {/* utility-class */}

}
