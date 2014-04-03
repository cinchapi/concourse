/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.cinchapi.concourse.time.Time;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.primitives.Longs;

/**
 * A wrapper class for a Unix timestamp with microsecond precision. A
 * {@link Timestamp} is required for historical operations in {@link Concourse}.
 * This class provides interoperability with Joda {@link DateTime} objects with
 * the {@link #fromJoda(DateTime)} and {@link #getJoda()} methods.
 * 
 * @author jnelson
 */
@Immutable
@ThreadSafe
public final class Timestamp {

    // Joda DateTime uses millisecond instead of microsecond precision, so this
    // class is a wrapper that will handle microseconds so we don't ever lose
    // data that happens within the same millisecond.

    /**
     * Return the {@code Timestamp} that corresponds to the provided joda
     * DateTime object.
     * 
     * @param joda
     * @return the timestamp for {@code joda}
     */
    public static Timestamp fromJoda(DateTime joda) {
        return new Timestamp(joda);
    }

    /**
     * Return a {@code Timestamp} that corresponds to the provided Unix
     * timestamp with microsecond precision.
     * 
     * @param microseconds
     * @return the timestamp for {@code microseconds}
     */
    public static Timestamp fromMicros(long microseconds) {
        return new Timestamp(microseconds);
    }

    /**
     * Return a {@code Timestamp} set the current system microsecond time using
     * <code>ISOChronology</code> in the default time zone.
     * 
     * @return the current timestamp, not null
     */
    public static Timestamp now() {
        return new Timestamp(Time.now());
    }

    /**
     * Return a {@code Timestamp} set to the current system microsecond time
     * using the specified chronology.
     * 
     * @param chronology the chronology, not null
     * @return the current timestamp, not null
     */
    public static Timestamp now(Chronology chronology) {
        long microseconds = Time.now();
        return new Timestamp(microseconds, new DateTime(
                TimeUnit.MILLISECONDS.convert(microseconds,
                        TimeUnit.MICROSECONDS), chronology));
    }

    /**
     * Return a {@code Timestamp} set to the current system microsecond time
     * using <code>ISOChronology</code> in the specified time zone.
     * 
     * @param zone the time zone, not null
     * @return the current timestamp, not null
     */
    public static Timestamp now(DateTimeZone zone) {
        long microseconds = Time.now();
        return new Timestamp(microseconds, new DateTime(
                TimeUnit.MILLISECONDS.convert(microseconds,
                        TimeUnit.MICROSECONDS), zone));
    }

    /**
     * Parses a {@code Timestamp} from the specified string.
     * <p>
     * This uses {@link ISODateTimeFormat#dateTimeParser()}.
     * 
     * @param str the string to parse, not null
     * @return the parsed timestamp
     */
    public static Timestamp parse(String str) {
        return new Timestamp(DateTime.parse(str, ISODateTimeFormat
                .dateTimeParser().withOffsetParsed()));
    }

    /**
     * Parses a {@code Timestamp} from the specified string using a formatter.
     * 
     * @param str the string to parse, not null
     * @param formatter the formatter to use, not null
     * @return the parsed timestamp
     */
    public static Timestamp parse(String str, DateTimeFormatter formatter) {
        return new Timestamp(DateTime.parse(str, formatter));
    }

    private final long microseconds;
    private final DateTime joda;

    /**
     * Construct a new instance.
     * 
     * @param joda
     */
    private Timestamp(DateTime joda) {
        this.joda = joda;
        this.microseconds = TimeUnit.MICROSECONDS.convert(joda.getMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Construct a new instance.
     * 
     * @param microseconds
     */
    private Timestamp(long microseconds) {
        this.microseconds = microseconds;
        this.joda = new DateTime(TimeUnit.MILLISECONDS.convert(microseconds,
                TimeUnit.MICROSECONDS));
    }

    /**
     * Construct a new instance.
     * 
     * @param microseconds
     * @param joda
     */
    private Timestamp(long microseconds, DateTime joda) {
        this.microseconds = microseconds;
        this.joda = joda;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Timestamp) {
            return Longs.compare(microseconds, ((Timestamp) obj).microseconds) == 0;
        }
        return false;
    }

    /**
     * Return the Joda {@link DateTime} object that corresponds to this
     * Timestamp.
     * 
     * @return the corresponding joda DateTime
     */
    public DateTime getJoda() {
        return joda;
    }

    /**
     * Return the number of microseconds since the Unix epoch that is
     * represented by this Timestamp.
     * 
     * @return the microseconds
     */
    public long getMicros() {
        return microseconds;
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(microseconds);
    }

    @Override
    public String toString() {
        return joda.toString(DateTimeFormat.forPattern("E MMM dd, yyyy @ h:mm:ss:SS a z"));
    }

}
