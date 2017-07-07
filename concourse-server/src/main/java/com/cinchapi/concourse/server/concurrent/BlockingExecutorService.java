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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.time.Time;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link BlockingExecutorService} is a wrapper around a normal
 * ExecutorService that allows caller threads to specify a batch of tasks to
 * complete before proceeding.
 * <p>
 * This service is designed to be used by multiple concurrent threads. While
 * each call to the {@link #execute(Callable...) execute} methods will block the
 * calling thread until all the specified tasks have completed, there are no
 * guarantees made about the order in which tasks will execute. That is because
 * all tasks execute asynchronously (just as they would in most other
 * ExecutorServices), but this wrapper has logic to make the calling thread
 * block until all of the tasks it cares about have executed.
 * </p>
 * <p>
 * Since tasks execute asynchronously, threads do not interfere with one
 * another. It is possible for thread A to submit tasks to the service before
 * thread B but have thread B's tasks finish before thread A's.
 * </p>
 * <p>
 * <strong>NOTE:</strong> All the threads used by this service are daemon
 * threads, so they don't need to be stopped explicitly. The downside to this is
 * that it is possible for the JVM to shutdown while this service is in the
 * middle of executing task. This shouldn't be a problem in reality, since
 * calling threads block while tasks are executing.
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class BlockingExecutorService {

    /**
     * Return a new {@link BlockingExecutorService} that uses a system specified
     * thread name prefix.
     * 
     * @return the BlockingExecutorService
     */
    public static BlockingExecutorService create() {
        return create("blocking-executor-service-" + Time.now());
    }

    /**
     * Return a new {@link BlockingExecutorService} that uses the specified
     * {@code threadNamePrefix}.
     * 
     * @param threadNamePrefix
     * @return the BlockingExecutorService
     */
    public static BlockingExecutorService create(String threadNamePrefix) {
        return new BlockingExecutorService(threadNamePrefix);
    }

    /**
     * Spin (and therefore block the current thread) until all the tasks
     * represented by the {@code futures} are done and the results are
     * available.
     * 
     * @param futures
     */
    private static void waitForCompletion(Future<?>... futures) {
        for (Future<?> future : futures) { 
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * The underlying Executor that actually accomplishes task execution.
     */
    private final ExecutorService executor;

    /**
     * Construct a new instance.
     * 
     * @param threadNamePrefix
     */
    private BlockingExecutorService(String threadNamePrefix) {
        this.executor = Executors
                .newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat(threadNamePrefix + "-%d").build());
    }

    /**
     * Execute all the tasks in the {@code batch} and block until all of them
     * complete. Afterwards, return the {@link Future} objects that represent
     * the results of execution. The results are returned in the same order that
     * the tasks are specified.
     * <p>
     * Please note that this method will block the calling thread until all the
     * tasks in the batch have completed, but it has no affect on other threads.
     * Therefore, it is possible that another thread can submit a batch of tasks
     * to the service and have them finish before the current thread's task do.
     * </p>
     * 
     * @param batch
     * @return the Future results of each task in the batch
     */
    public List<Future<?>> execute(Callable<?>... batch) {
        Future<?>[] futures = new Future<?>[batch.length];
        for (int i = 0; i < futures.length; ++i) {
            futures[i] = executor.submit(batch[i]);
        }
        waitForCompletion(futures);
        return Lists.newArrayList(futures);
    }

    /**
     * Execute all the tasks in the {@code batch} and block until all of them
     * complete.
     * <p>
     * Please note that this method will block the calling thread until all the
     * tasks in the batch have completed, but it has no affect on other threads.
     * Therefore, it is possible that another thread can submit a batch of tasks
     * to the service and have them finish before the current thread's task do.
     * </p>
     * 
     * @param batch
     */
    public void execute(Runnable... batch) {
        Future<?>[] futures = new Future<?>[batch.length];
        for (int i = 0; i < futures.length; ++i) {
            futures[i] = executor.submit(batch[i]);
        }
        waitForCompletion(futures);
    }
}
