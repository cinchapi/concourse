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
package com.cinchapi.concourse.server.time;

import java.util.concurrent.TimeUnit;

import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.ensemble.clock.Clock;

/**
 * A bridge between {@link Clock clocks} from the distributed framework and
 * Concourse's microsecond-precision time requirements. This class provides
 * consistent time interpretation by converting various time formats to the
 * microsecond precision needed by Concourse.
 * <p>
 * For internal Concourse components that need time values, always use the
 * {@link #epochMicros()} method to get a microsecond timestamp. When receiving
 * timestamps via the distributed framework, use the {@link #interpret(long)}
 * method to convert them into Concourse-compatible microsecond values.
 * 
 * @author Jeff Nelson
 */
public abstract class TimeSource implements Clock {

    /**
     * Return the canonical {@link TimeSource} for distributed environments.
     * 
     * @return the distributed {@link TimeSource} instance
     * @throws UnsupportedOperationException if a different time source is
     *             already in use
     */
    public static TimeSource distributed() {
        if(LOCAL != null) {
            throw new UnsupportedOperationException(
                    "A different time source has already been created");
        }
        if(DISTRIBUTED == null) {
            DISTRIBUTED = new DistributedTimeSource();
        }
        return DISTRIBUTED;
    }

    /**
     * Return the singleton instance of the {@link TimeSource}.
     * 
     * @return the {@link TimeSource} instance
     */
    public static TimeSource get() {
        TimeSource instance = AnyObjects.firstThat(s -> s != null, LOCAL,
                DISTRIBUTED);
        if(instance == null) {
            LOCAL = new LocalTimeSource();
            instance = LOCAL;
        }
        return instance;
    }

    /**
     * Return the canonical {@link TimeSource} that uses the local system clock.
     * 
     * @return the local {@link TimeSource} instance
     * @throws UnsupportedOperationException if a different time source is
     *             already in use
     */
    public static TimeSource local() {
        if(DISTRIBUTED != null) {
            throw new UnsupportedOperationException(
                    "A different time source has already been created");
        }
        if(LOCAL == null) {
            LOCAL = new LocalTimeSource();
        }
        return LOCAL;
    }

    /**
     * The singleton instance of local {@link TimeSource}.
     */
    private static TimeSource LOCAL = null;;

    /**
     * The singleton instance of distributed {@link TimeSource}.
     */
    private static TimeSource DISTRIBUTED = null;

    /**
     * Construct a new instance.
     */
    private TimeSource() {/* no-init */}

    /**
     * Return the current time in microseconds since the epoch.
     * 
     * @return the current time in microseconds
     */
    public long epochMicros() {
        return interpret(time());
    }

    /**
     * Interpret a raw timestamp value and convert it to microseconds. Different
     * implementations may apply different conversion logic based on the origin
     * of the timestamp.
     * 
     * @param time the raw timestamp to interpret
     * @return the interpreted time in microseconds
     */
    public abstract long interpret(long time);

    /**
     * A {@link TimeSource} for distributed environments that uses a
     * {@link Clock#hybrid(Clock) hybrid logical clock} based on the
     * {@link Clock#ntp() Network Time Protocol}.
     * 
     * @author Jeff Nelson
     */
    static class DistributedTimeSource extends ForwardingTimeSource {

        /**
         * Construct a new instance.
         */
        DistributedTimeSource() {
            super(Clock.hybrid(Clock.ntp()));
        }

        @Override
        public long interpret(long time) {
            // The high order 48-bits of a hybrid timestamp contain the clock
            // timestamp.
            long millis = (time >>> 16) & 0xFFFFFFFFFFFFL;
            return TimeUnit.MICROSECONDS.convert(millis, TimeUnit.MILLISECONDS);
        }

    }

    /**
     * A {@link TimeSource} implementation that uses the local system time.
     * 
     * @author Jeff Nelson
     */
    static class LocalTimeSource extends TimeSource {

        @Override
        public long interpret(long time) {
            return time;
        }

        @Override
        public long time() {
            return Time.now();
        }

    }

    /**
     * A {@link TimeSource} that forwards time requests to another {@link Clock}
     * implementation.
     * 
     * @author Jeff Nelson
     */
    private static abstract class ForwardingTimeSource extends TimeSource {

        /**
         * The underlying {@link Clock} that provides the time.
         */
        protected final Clock clock;

        /**
         * Construct a new instance.
         * 
         * @param clock
         */
        ForwardingTimeSource(Clock clock) {
            this.clock = clock;
        }

        @Override
        public final void sync(long time) {
            clock.sync(time);
        }

        @Override
        public final long time() {
            return clock.time();
        }
    }

}
