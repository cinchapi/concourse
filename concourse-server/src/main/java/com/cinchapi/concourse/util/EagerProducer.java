/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.common.base.CheckedExceptions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link EagerProducer} creates and queues up expensive elements ahead of
 * time in anticipation of providing them to a consumer.
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class EagerProducer<T> {

    /**
     * Return a new {@link EagerProducer} that uses the {@code supplier} to
     * produce elements that can be {@link #consume() consumed}.
     * 
     * @param supplier
     * @return the {@link EagerProducer}
     */
    public static <T> EagerProducer<T> of(Supplier<T> supplier) {
        return new EagerProducer<T>(supplier);
    }

    /**
     * The queue that holds that next element to be transferred to the consumer.
     */
    private final SynchronousQueue<T> queue = new SynchronousQueue<T>();

    /**
     * Construct a new instance.
     * 
     * @param supplier
     */
    public EagerProducer(final Supplier<T> supplier) {
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                try {
                    for (;;) {
                        T element = supplier.get();
                        queue.put(element);
                    }
                }
                catch (Exception e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
            }

        };

        // Create threads that will continuously try to insert elements into the
        // queue, blocking until one of the elements is taken.
        int count = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(count,
                new ThreadFactoryBuilder().setDaemon(true).build());
        for (int i = 0; i < count; ++i) {
            executor.execute(runnable);
        }

    }

    /**
     * Consume the next element from this Producer.
     * 
     * @return the element
     */
    public T consume() {
        try {
            return queue.take();
        }
        catch (Exception e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

}
