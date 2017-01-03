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

import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.Timestamp;
import com.google.common.primitives.Longs;
import com.joestelmach.natty.DateGroup;

import ch.qos.logback.classic.Level;

/**
 * A collection of utility functions for processing natural language directives.
 * 
 * @author Jeff Nelson
 */
public final class NaturalLanguage {

    /**
     * Parse the number of microseconds from the UNIX epoch that are described
     * by {@code str}.
     * 
     * @param str
     * @return the microseconds
     */
    public static long parseMicros(String str) {
        // We should assume that the timestamp is in microseconds since
        // that is the output format used in ConcourseShell
        Long micros = Longs.tryParse(str);
        if(micros != null) {
            return micros;
        }
        else {
            try {
                return Timestamp.fromJoda(
                        Timestamp.DEFAULT_FORMATTER.parseDateTime(str))
                        .getMicros();
            }
            catch (Exception e) {
                List<DateGroup> groups = TIMESTAMP_PARSER.parse(str);
                Date date = null;
                for (DateGroup group : groups) {
                    date = group.getDates().get(0);
                    break;
                }
                if(date != null) {
                    return Timestamp.fromJoda(new DateTime(date)).getMicros();
                }
                else {
                    throw new IllegalStateException(
                            "Unrecognized date/time string '" + str + "'");
                }
            }
        }
    }

    /**
     * A parser to convert natural language text strings to Timestamp objects.
     */
    private final static com.joestelmach.natty.Parser TIMESTAMP_PARSER = new com.joestelmach.natty.Parser();

    static {
        // Turn off logging in 3rd party code
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger("com.joestelmach")).setLevel(Level.OFF);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("net.fortuna"))
                .setLevel(Level.OFF);
    }

    private NaturalLanguage() {/* noop */}

}
