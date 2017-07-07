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
package com.cinchapi.concourse.time;

/**
 * Time utilities.
 * 
 * @author Jeff Nelson
 */
public final class Time {

    /**
     * An indication that a timestamp parameter should be ignored.
     */
    public static final long NONE = Long.MAX_VALUE;

    private static AtomicClock clock = new AtomicClock();

    /**
     * Get the current timestamp. Use this throughout a project to make sure
     * that no time collisions happen.
     * 
     * @return the current timestamp.
     */
    public static long now() {
        return clock.time();
    }

}
