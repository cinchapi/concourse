/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import com.cinchapi.common.base.CheckedExceptions;
import com.google.common.util.concurrent.ForwardingExecutorService;

/**
 * An {@link AwaitableExecutorService} generally delegates to another
 * {@link ExecutorService} while adding functionality to submit multiple
 * {@link Runnable tasks} and {@link #await(BiConsumer, Runnable...)} the result
 * with configurable error handling.
 *
 * @author Jeff Nelson
 */
public class AwaitableExecutorService extends ForwardingExecutorService {

    /**
     * The {@link ExecutorService} that does all the work.
     */
    private final ExecutorService delegate;

    /**
     * Construct a new instance.
     * 
     * @param delegate
     */
    public AwaitableExecutorService(ExecutorService delegate) {
        this.delegate = delegate;
    }

    @Override
    protected ExecutorService delegate() {
        return delegate;
    }

    /**
     * Atomically submit each of the {@link tasks} for execution and await their
     * completion. If an error occurs if any of the tasks, the entire operation
     * is considered to have {@code failed}.
     * 
     * @param tasks
     * @return a boolean that indicates whether all of the tasks have completed
     *         successfully
     * @throws InterruptedException
     */
    public final boolean await(Runnable... tasks) throws InterruptedException {
        return await((task, error) -> {
            throw CheckedExceptions.wrapAsRuntimeException(error);
        }, tasks);
    }

    /**
     * Atomically submit each of the {@link tasks} for execution and await their
     * completion. If an error occurs if any of the tasks, the entire operation
     * is considered to have {@code failed} and exceptions are handled by the
     * provided {@code errorHandler}.
     * 
     * @param errorHandler
     * @param tasks
     * @return a boolean that indicates whether all of the tasks have completed
     *         successfully
     * @throws InterruptedException
     */
    public final boolean await(BiConsumer<Runnable, Throwable> errorHandler,
            Runnable... tasks) throws InterruptedException {
        Future<?>[] futures = new Future<?>[tasks.length];
        for (int i = 0; i < tasks.length; ++i) {
            Runnable task = tasks[i];
            futures[i] = delegate.submit(task);
        }
        boolean success = true;
        for (int i = 0; i < futures.length; ++i) {
            Future<?> future = futures[i];
            try {
                future.get();
            }
            catch (ExecutionException e) {
                Runnable task = tasks[i];
                success = false;
                errorHandler.accept(task, e);
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
            catch (CancellationException e) {
                success = false;
            }
        }
        return success;
    }

}
