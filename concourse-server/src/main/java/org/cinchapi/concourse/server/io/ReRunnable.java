/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.io;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link Runnable} instance that can be reused. Each {@link ReRunnable}
 * operates on an {@link #object}. Between reruns, you can specify the object to
 * use using the {@link #runWith(Object)} method.
 * 
 * @author jnelson
 */
@NotThreadSafe
public abstract class ReRunnable<T> implements Runnable {

    /**
     * The object to run with. The implementing subclass is epected to use this
     * value in the {@link #run()} metho.
     */
    protected T object;

    /**
     * Specify the object to use in the next invocation of the
     * {@link #runWith(Object)} method. This method returns the current instance
     * for chaining purposes.
     * 
     * @param object
     * @return this
     */
    public ReRunnable<T> runWith(T object) {
        this.object = object;
        return this;
    }

}
