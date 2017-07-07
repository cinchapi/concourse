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
package com.cinchapi.concourse.server.io.process;

import com.google.common.base.Throwables;

/**
 * A {@link Callback} is invoked with the result of some asynchronous method
 * that is usually
 * {@link com.cinchapi.concourse.server.io.process.ServerProcesses forked} to an
 * external JVM.
 * 
 * @author Jeff Nelson
 */
public abstract class Callback<T> {

    /**
     * The result passed to the callback.
     */
    private volatile T result;

    /**
     * An internal signaler to know when the result is available.
     */
    private Object signal = new Object();

    /**
     * Specify the actions to perform with the {@code result} of an asynchronous
     * method.
     * 
     * @param result the result to handle
     */
    public abstract void onResult(T result);

    /**
     * Supply the result to the callback.
     * 
     * @param result the result
     */
    void result(T result) {
        this.result = result;
        onResult(result);
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    /**
     * Get the result passed to the callback, blocking if necessary until the
     * result is available.
     * 
     * @return the result
     */
    public T getResult() {
        if(result == null) {
            synchronized (signal) {
                try {
                    while (result == null) {
                        signal.wait();
                    }
                }
                catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        return result;
    }

}
