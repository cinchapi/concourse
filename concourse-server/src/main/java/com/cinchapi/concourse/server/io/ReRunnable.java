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
package com.cinchapi.concourse.server.io;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link Runnable} instance that can be reused. Each {@link ReRunnable}
 * operates on an {@link #object}. Between reruns, you can specify the object to
 * use using the {@link #runWith(Object)} method.
 * 
 * @author Jeff Nelson
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
