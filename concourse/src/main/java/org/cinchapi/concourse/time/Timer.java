/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.time;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * A basic timer.
 * 
 * @author jnelson
 */
public class Timer {

    private final static int defaultStartTime = 0;
    private final static TimeUnit defaultTimeUnit = TimeUnit.MICROSECONDS;
    private long start = defaultStartTime;

    /**
     * Start the timer now.
     */
    public void start() {
        Preconditions.checkState(start == defaultStartTime,
                "The timer has already been started.");
        start = Time.now();
    }

    /**
     * Stop the timer now and return the amount of time measured in
     * {@code milliseconds} that has elapsed.
     * 
     * @return the elapsed time.
     */
    public long stop() {
        return stop(defaultTimeUnit);
    }

    /**
     * Stop the timer now and return the amount of time measured in {@code unit}
     * that has elapsed.
     * 
     * @param unit
     * @return the elapsed time.
     */
    public long stop(TimeUnit unit) {
        Preconditions.checkArgument(start != defaultStartTime,
                "The timer has not been started.");
        long duration = Time.now() - start;
        start = defaultStartTime;
        return unit.convert(duration, defaultTimeUnit);
    }

}
