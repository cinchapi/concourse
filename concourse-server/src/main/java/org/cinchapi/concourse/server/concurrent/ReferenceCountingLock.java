/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.cinchapi.concourse.annotate.PackagePrivate;

/**
 * A {@link ReentrantReadWriteLock} that has a counter to keep track of
 * references. References are decremented when the read or write lock is
 * released. The subclass should increment the reference count when the lock is
 * considered to be <em>grabbed</em>
 * 
 * @author jnelson
 */
@PackagePrivate
@SuppressWarnings("serial")
class ReferenceCountingLock extends ReentrantReadWriteLock {

    // NOTE: This class does not define hashCode() or equals() because the
    // defaults are the desired behaviour

    /**
     * A counter that keeps track of "references" to this lock. Each time
     * the lock is requested, the counter is incremented. Each time the lock
     * is released, the counter is decremented. This counter helps to
     * perform periodic cleanup.
     */
    @PackagePrivate
    final AtomicInteger refs = new AtomicInteger(0);

    @Override
    public ReadLock readLock() {
        return new ReadLock(this) {

            @Override
            public void unlock() {
                super.unlock();
                refs.decrementAndGet();
            }

        };
    }

    @Override
    public WriteLock writeLock() {
        return new WriteLock(this) {

            @Override
            public void unlock() {
                super.unlock();
                refs.decrementAndGet();
            }

        };
    }
}