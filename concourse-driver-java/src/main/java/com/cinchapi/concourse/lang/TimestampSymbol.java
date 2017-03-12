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
package com.cinchapi.concourse.lang;

import com.cinchapi.concourse.Timestamp;

/**
 * A {@link Symbol} that represents a {@link Timestamp} in a {@link Criteria}.
 * 
 * @author Jeff Nelson
 */
public class TimestampSymbol extends AbstractSymbol {

    /**
     * Return the {@link TimestampSymbol} for the specified {@code timestamp}.
     * 
     * @param timestamp
     * @return the Symbol
     */
    public static TimestampSymbol create(Timestamp timestamp) {
        return new TimestampSymbol(timestamp);
    }

    /**
     * Return the {@link TimestampSymbol} for the specified {@code timestamp}.
     * 
     * @param timestamp
     * @return the Symbol
     */
    public static TimestampSymbol create(long timestamp) {
        return new TimestampSymbol(timestamp);
    }

    /**
     * Return the {@link TimestampSymbol} that is parsed from {@code string}.
     * 
     * @param string
     * @return the Symbol
     */
    public static TimestampSymbol parse(String string) {
        return new TimestampSymbol(Long.parseLong(string.replace("at ", "")));
    }

    /**
     * The associated timestamp.
     */
    private final long timestamp;

    /**
     * Construct a new instance.
     * 
     * @param timestamp
     */
    private TimestampSymbol(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Construct a new instance.
     * 
     * @param timestamp
     */
    private TimestampSymbol(Timestamp timestamp) {
        this(timestamp.getMicros());
    }

    /**
     * Return the timestamp (in microseconds) associated with this Symbol.
     * 
     * @return the Timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "at " + Long.toString(timestamp);
    }

}
