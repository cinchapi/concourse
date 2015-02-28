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
package org.cinchapi.concourse.server.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.cinchapi.concourse.annotate.UtilityClass;
import org.cinchapi.concourse.util.Logger;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A collection of thread and concurrency related utility methods.
 * 
 * @author jnelson
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
        ExecutorService executor = Executors
                .newFixedThreadPool(commands.length);
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
                .setNameFormat(threadNamePrefix + "-%d")
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
