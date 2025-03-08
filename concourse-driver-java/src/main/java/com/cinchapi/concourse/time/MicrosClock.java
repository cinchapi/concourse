/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
 * A {@link MicrosClock} provides a mechanism for tracking time with microsecond
 * standard millisecond-precision clocks, this interface ensures finer time
 * granularity, which is useful for high-resolution time measurements, event
 * sequencing, and performance monitoring.
 * <p>
 * Implementations of this interface should provide a consistent and efficient
 * method for retrieving the current epoch time in microseconds.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface MicrosClock {

    /**
     * Returns the current time in microseconds since the UNIX epoch
     * (January 1, 1970, 00:00:00 UTC).
     * <p>
     * Implementations must ensure that the returned value is a strictly
     * increasing timestamp with a resolution of at least one microsecond.
     * </p>
     * 
     * @return the current epoch time in microseconds
     */
    public long epochMicros();

    /**
     * Returns the current time in microseconds since the UNIX epoch
     * (January 1, 1970, 00:00:00 UTC).
     * <p>
     * Implementations must ensure that the returned value is a strictly
     * increasing timestamp with a resolution of at least one microsecond.
     * </p>
     * 
     * @return the current epoch time in microseconds
     */
    public default long currentTimeMicros() {
        return epochMicros();
    }

}
