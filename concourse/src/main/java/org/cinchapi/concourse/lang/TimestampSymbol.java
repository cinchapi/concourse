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
package org.cinchapi.concourse.lang;

import org.cinchapi.concourse.Timestamp;

/**
 * A {@link Symbol} that represents a {@link Timestamp} in a {@link Criteria}.
 * 
 * @author jnelson
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
