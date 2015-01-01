/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Throwables;

/**
 * A {@link ConcurrentLinkedQueue} that uses a specified supplier
 * {@link Callable} to dynamically loading elements into the queue when a read
 * request is made and the queue is empty.
 * 
 * @author jnelson
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
