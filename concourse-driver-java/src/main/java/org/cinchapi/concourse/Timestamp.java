/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse;

import java.util.Date;
import java.util.List;
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
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.common.primitives.Longs;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

/**
 * A wrapper class for a Unix timestamp with microsecond precision. A
 * {@link Timestamp} is required for historical operations in {@link Concourse}.
 * This class provides interoperability with Joda {@link DateTime} objects with
 * the {@link #fromJoda(DateTime)} and {@link #getJoda()} methods.
 * 
 * @author Jeff Nelson
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
     * Return a {@code Timestamp} that corresponds to the system
     * epoch timestamp with microsecond precision.
     * 
     * @return the timestamp for system epoch
     */
    public static Timestamp epoch() {
        return Timestamp.fromMicros(-1);
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
        if(Longs.tryParse(str) != null) {
            // We should assume that the timestamp is in microseconds since
            // that is the output format used in ConcourseShell
            return fromMicros(Long.parseLong(str));
        }
        else {
            List<DateGroup> groups = NLP.parse(str);
            Date date = null;
            for (DateGroup group : groups) {
                date = group.getDates().get(0);
                break;
            }
            if(date != null) {
                return fromJoda(new DateTime(date));
            }
            else {
                throw new IllegalArgumentException(
                        "Unrecognized date/time string '" + str + "'");
            }
        }
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

    /**
     * A parser to convert natural language text strings to Timestamp objects.
     */
    private final static Parser NLP = new Parser();
    static{
        // Turn off logging in 3rd party code
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("com.joestelmach")).setLevel(Level.OFF);
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("net.fortuna")).setLevel(Level.OFF);
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
        return joda.toString(DateTimeFormat
                .forPattern("E MMM dd, yyyy @ h:mm:ss:SS a z"));
    }

}
