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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Throwables;

/**
 * A {@link ConcurrentLinkedQueue} that uses a specified supplier
 * {@link Callable} to dynamically load elements into the queue when a read
 * request is made and the queue is empty.
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
public class ConcurrentLoadingQueue<E> extends ConcurrentLinkedQueue<E> {

    /**
     * Return a new {@link ConcurrentLoadingQueue} that uses the
     * {@code supplier} to populate the queue on demand.
     * 
     * @param supplier
     * @return the ConcurrentLoadingQueue
     */
    public static <E> ConcurrentLoadingQueue<E> create(Callable<E> supplier) {
        return new ConcurrentLoadingQueue<E>(supplier);
    }

    /**
     * Return a new {@link ConcurrentLoadingQueue} that initially contains the
     * elements of the given {@code collection} in traversal order of the
     * collection's iterator and uses the {@code supplier} to populate the queue
     * on demand.
     * 
     * @param collection
     * @param supplier
     * @return the ConcurrentLoadingQueue
     */
    public static <E> ConcurrentLoadingQueue<E> create(
            Collection<E> collection, Callable<E> supplier) {
        ConcurrentLoadingQueue<E> queue = create(supplier);
        for (E element : collection) {
            queue.offer(element);
        }
        return queue;
    }

    private static final long serialVersionUID = 1L;

    /**
     * The function that supplies elements to the queue when it is empty.
     */
    private final Callable<E> supplier;

    /**
     * Construct a new instance.
     * 
     * @param supplier
     */
    private ConcurrentLoadingQueue(Callable<E> supplier) {
        this.supplier = supplier;
    }

    @Override
    public E peek() {
        E element = super.peek();
        if(element == null) {
            loadElement();
            return super.peek();
        }
        else {
            return element;
        }
    }

    @Override
    public E poll() {
        E element = super.poll();
        if(element == null) {
            loadElement();
            return super.poll();
        }
        else {
            return element;
        }
    }

    /**
     * Load a new element and place it into the queue.
     */
    private void loadElement() {
        try {
            E element = supplier.call();
            offer(element);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
