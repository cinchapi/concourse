/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Static factory methods for returning objects that implement the {@link Queue}
 * interface.
 * 
 * @author Jeff Nelson
 */
public final class Queues {

    /**
     * Return a simple {@link Queue} that can be used in a single thread.
     * 
     * @return the {@link Queue}
     */
    public static <T> Queue<T> newSingleThreadedQueue() {
        return new ArrayDeque<T>();
    }

    /**
     * Perform the {@link BlockingQueue#drainTo(Collection)} operation, but
     * block indefinitely until at least one element is available.
     * 
     * @param queue the {@link BlockingQueue} in which to perform the
     *            {@link BlockingQueue#drainTo(Collection) drainTo} method
     * @param buffer the collection into which the elements are drained
     * @return the number of elements that are drained
     * @throws InterruptedException
     */
    public static <E> int blockingDrain(BlockingQueue<E> queue,
            Collection<? super E> buffer) throws InterruptedException {
        Preconditions.checkNotNull(buffer);
        int added = queue.drainTo(buffer);
        if(added == 0) {
            // If the initial drain doesn't return any elements, we must
            // call #take in order to block until at least on element is
            // available
            buffer.add(queue.take());
            added += queue.drainTo(buffer);
            ++added;
        }
        return added;
    }

    /**
     * Perform the {@link BlockingQueue#drainTo(Collection)} operation, but
     * block for the specified {@code timeout} until at least one element is
     * available.
     * 
     * @param queue the {@link BlockingQueue} in which to perform the
     *            {@link BlockingQueue#drainTo(Collection) drainTo} method
     * @param buffer the collection into which the elements are drained
     * @return the number of elements that are drained
     */
    public static <E> int blockingDrain(BlockingQueue<E> queue,
            Collection<? super E> buffer, long timeout, TimeUnit unit) {
        Preconditions.checkNotNull(buffer);
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int added = queue.drainTo(buffer);
        try {
            if(added == 0) {
                E element = queue.poll(deadline - System.nanoTime(),
                        TimeUnit.NANOSECONDS);
                if(element != null) {
                    // If the element is null, it means we've waited long enough
                    // and no element has been added; otherwise, we need to
                    // check if more than one element was added
                    buffer.add(element);
                    added += queue.drainTo(buffer);
                    ++added;
                }
            }
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        return added;
    }

    private Queues() {/* noop */}

}
