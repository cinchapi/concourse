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

import java.util.concurrent.TimeUnit;

import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.ensemble.clock.Clock;

/**
 * A bridge between a {@link Clock}, which tells {@link Clock#time() time} in
 * epoch milliseconds, and Concourse's {@link MicrosClock microsecond-precision
 * time requirements}.
 * The {@link TimeSource} abstraction allows for
 * <ol>
 * <li>
 * correctly getting
 * {@link #epochMicros() microsecond} timestamps based on the actual time of an
 * underlying {@link Clock clock}, and
 * </li>
 * <li>
 * {@link #interpret(long) interpreting} timestamps received from the underlying
 * {@link Clock clock} as a Concourse compatible microsecond timestamp
 * </li>
 * </ol>
 * <p>
 * For interchangeability, each {@link TimeSource} is an extension of a
 * {@link Clock} and the {@link Clock#time()} method always returns a
 * clock-native timestamp. Internal Concourse components should use the
 * {@link #epochMicros()} method to get Concourse timestamps.
 * </p>
 * <p>
 * In cases where a provided timestamp is known to have been generated using the
 * {@link #time()} method (e.g., in the distributed system framework}, internal
 * Concourse components should first {@link #interpret(long) convert} the
 * timestamp to microseconds before using it.
 * </p>
 * <p>
 * <strong>NOTE:</strong> Only one {@link TimeSource} can be active for a given
 * application runtime. Use {@link TimeSource#get()} to obtain the active
 * {@link TimeSource}. If none has been explicitly activated, {@link #get()}
 * will default to the {@link #local() local} {@link TimeSource}.
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class TimeSource implements Clock, MicrosClock {

    /**
     * Return the canonical {@link TimeSource} for distributed environments.
     * <p>
     * In a distributed system, clock drift can cause timestamps to diverge
     * across nodes, leading to inconsistencies where time may appear to move
     * backward when events from different nodes are consolidated. To mitigate
     * this issue, a distributed {@link TimeSource} ensures that time progresses
     * monotonically, preventing any node from observing non-sequential
     * timestamps.
     * </p>
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

    @Override
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

            // Configure Time utilities to use this time source for timestamp
            // generation without explicitly calling this TimeSource directly.
            Time.setClock(this);
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
     * <p>
     * This {@link TimeSource} relies on the default {@link Time} utilities and
     * assumes that any timestamps it {@link #interpret(long) interprets} were
     * directly or indirectly generated by those utilities.
     * </p>
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
     * implementation, but may {@link #interpret(long) interpret} timestamps
     * generated by that clock differently for internal compatibility.
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
