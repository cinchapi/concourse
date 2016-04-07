/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
import java.util.Queue;

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

    private Queues() {/* noop */}

}
