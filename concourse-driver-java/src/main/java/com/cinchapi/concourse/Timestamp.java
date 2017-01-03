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
package com.cinchapi.concourse;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.time.Time;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

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
     * Return a {@code Timestamp} that corresponds to the system
     * epoch timestamp with microsecond precision.
     * 
     * @return the timestamp for system epoch
     */
    public static Timestamp epoch() {
        return Timestamp.fromMicros(-1);
    }

    /**
     * Return the {@code Timestamp} that corresponds to the provided joda
     * DateTime object.
     * 
     * @param joda a {@link DateTime} object
     * @return the timestamp for {@code joda}
     */
    public static Timestamp fromJoda(DateTime joda) {
        return new Timestamp(joda);
    }

    /**
     * Return a {@code Timestamp} that corresponds to the provided Unix
     * timestamp with microsecond precision.
     * 
     * @param microseconds the number of microseconds since the Unix epoch
     * @return the timestamp for {@code microseconds}
     */
    public static Timestamp fromMicros(long microseconds) {
        return new Timestamp(microseconds);
    }

    /**
     * Take the {@code description} and return a {@link Timestamp} that can be
     * passed to {@link Concourse driver} API methods.
     * <p>
     * Timestamp description are parsed by Concourse Server, so this method only
     * returns a wrapper that is meant to be passed over the wire. Timestamps
     * returned from this method are <em>non-operable</em> and will throw
     * exceptions if you call methods that would return a precise instant (i.e.
     * {@link #getJoda()} or {@link #getMicros()}).
     * </p>
     * 
     * @param description a relative or absolute natural language description of
     *            an instant.
     * @return a hollow {@link Timestamp} that wraps the description
     */
    public static Timestamp fromString(String description) {
        return new Timestamp(description);
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
     * The default formatter that is used to display objects of this class.
     */
    public static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormat
            .forPattern("E MMM dd, yyyy @ h:mm:ss:SS a z");

    /**
     * A relative or absolute description of an instant that is translated to an
     * actual microsecond timestamp in Concourse Server. By convention, a
     * {@link Timestamp} object is considered to be {@link #isString() hollow}
     * if and only if the value of this variable is non-null.
     */
    private final String description;

    /**
     * A {@link DateTime} object that corresponds to this {@link Timestamp}. We
     * use this for the {@link #toString() string} representation and for any
     * other comparative operations that need to take things like timezones into
     * account.
     */
    private final DateTime joda;

    /**
     * The number of microseconds since the Unix epoch that identify the instant
     * represented by this {@link Timestamp}.
     */
    private final long microseconds;

    /**
     * Construct a new instance.
     * 
     * @param joda
     */
    private Timestamp(DateTime joda) {
        this.joda = joda;
        this.microseconds = TimeUnit.MICROSECONDS.convert(joda.getMillis(),
                TimeUnit.MILLISECONDS);
        this.description = null;
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
        this.description = null;
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
        this.description = null;
    }

    /**
     * Construct a {@link #isString()} instance.
     * 
     * @param description the description to be resolved into an instant by
     *            Concourse Server
     */
    private Timestamp(String description) {
        this.microseconds = 0;
        this.joda = null;
        this.description = description;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Timestamp && !isString()) {
            return Longs.compare(microseconds, ((Timestamp) obj).microseconds) == 0;
        }
        // NOTE: By convention, two hollow timestamps are NEVER equal
        return false;
    }

    /**
     * Return the Joda {@link DateTime} object that corresponds to this
     * Timestamp.
     * 
     * @return the corresponding joda DateTime
     */
    public DateTime getJoda() {
        Preconditions.checkState(!isString(),
                "Only Concourse Server can parse a DateTime "
                        + "from a Timestamp created from a string.");
        return joda;
    }

    /**
     * Return the number of microseconds since the Unix epoch that is
     * represented by this Timestamp.
     * 
     * @return the microseconds
     */
    public long getMicros() {
        Preconditions.checkState(!isString(),
                "Only Concourse Server can parse microseconds "
                        + "from a Timestamp created from a string.");
        return microseconds;
    }

    @Override
    public int hashCode() {
        return isString() ? description.hashCode() : Longs
                .hashCode(microseconds);
    }

    @Override
    public String toString() {
        return isString() ? description : joda.toString(DEFAULT_FORMATTER);
    }

    /**
     * The {@link com.cinchapi.concourse.thrift.ConcourseService thrift} API
     * allows specifying timestamps using either microseconds from the unix
     * epoch ( {@link Long long}) or a natural language description of an
     * absolute or relative instant ({@link String}). But we can't define
     * overloaded methods in the {@link Concourse driver} API that take a long
     * or String for the timestamp parameter because that signatures would be
     * ambiguous (i.e. does the method {@link Concourse#select(String, String)}
     * mean {@code select(key, ccl)} or {@code select(ccl, timestring)}?).
     * <p>
     * Therefore, we allow a {@link Timestamp} to be created
     * {@link #fromString(String) from a string description}, which will be
     * translated and resolved by Concourse Server. But these objects are
     * considered to be hollow because the driver and client code is unable to
     * use the objects in any way.
     * </p>
     * <p>
     * For a hollow Timestamp, use the {@link #toString()} method to get the
     * description.
     * </p>
     * 
     * @return {@code true} if the timestamp was created
     *         {@link #fromString(String) using a description}
     */
    @PackagePrivate
    boolean isString() {
        return description != null;
    }

}
