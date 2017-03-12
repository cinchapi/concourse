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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that gives as close to the current time as possible without
 * duplicates in microseconds.
 * 
 * @author Jeff Nelson
 * 
 */
public class AtomicClock {

    private final AtomicLong time = new AtomicLong();

    /**
     * Get the current timestamp (in microseconds), which is guaranteed to be
     * unique.
     * 
     * @return the timestamp.
     */
    public long time() {
        long now = System.currentTimeMillis() * 1000;
        for (;;) {
            long lastTime = time.get();
            if(lastTime >= now) {
                now = lastTime + 1;
            }
            if(time.compareAndSet(lastTime, now)) {
                return now;
            }
        }
    }
}
