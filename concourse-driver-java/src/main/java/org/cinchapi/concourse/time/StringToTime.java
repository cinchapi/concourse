/*
 * Copyright (c) 2012 Clutch, Inc.
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package org.cinchapi.concourse.time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * A Java implementation of the PHP function strtotime(String, int): accepts
 * various expressions of time
 * in <code>String</code> format, and returns a timestamp or {@link DateTime}
 * object.
 * 
 * <h2>Valid expressions of time</h2>
 * 
 * <p>
 * All expressions are case-insensitive.
 * </p>
 * 
 * <ul>
 * <li><code>now</code> (equal to <code>new Date()</code>)</li>
 * <li><code>today</code> (equal to <code>StringToTime("00:00:00.000")</code>)</li>
 * <li><code>midnight</code> (equal to
 * <code>StringToTime("00:00:00.000 +24 hours")</code>)</li>
 * <li><code>morning</code> or <code>this morning</code> (by default, equal to
 * <code>StringToTime("07:00:00.000")</code>)</li>
 * <li><code>noon</code> (by default, equal to
 * <code>StringToTime("12:00:00.000")</code></li>
 * <li><code>afternoon</code> or <code>this afternoon</code> (by default, equal
 * to <code>StringToTime("13:00:00.000")</code></li>
 * <li><code>evening</code> or <code>this evening</code> (by default, equal to
 * <code>StringToTime("17:00:00.000")</code></li>
 * <li><code>tonight</code> (by default, equal to
 * <code>StringToTime("20:00:00.000")</code></li>
 * <li><code>tomorrow</code> (by default, equal to
 * <code>StringToTime("now +24 hours")</code>)</li>
 * <li><code>tomorrow morning</code> (by default, equal to
 * <code>StringToTime("morning +24 hours")</code>)</li>
 * <li><code>noon tomorrow</code> or <code>tomorrow noon</code> (by default,
 * equal to <code>StringToTime("noon +24 hours")</code>)</li>
 * <li><code>tomorrow afternoon</code> (by default, equal to
 * <code>StringToTime("afternoon +24 hours")</code>)</li>
 * <li><code>yesterday</code> (by default, equal to
 * <code>StringToTime("now -24 hours")</code>)</li>
 * <li>all the permutations of <code>yesterday</code> and <code>morning</code>,
 * <code>noon</code>, <code>afternoon</code>, and <code>evening</code></li>
 * <li><code>October 26, 1981</code> or <code>Oct 26, 1981</code></li>
 * <li><code>October 26</code> or <code>Oct 26</code></li>
 * <li><code>26 October 1981</code></li>
 * <li><code>26 Oct 1981</code></li>
 * <li><code>26 Oct 81</code></li>
 * <li><code>10/26/1981</code> or <code>10-26-1981</code></li>
 * <li><code>10/26/81</code> or <code>10-26-81</code></li>
 * <li><code>1981/10/26</code> or <code>1981-10-26</code></li>
 * <li><code>10/26</code> or <code>10-26</code></li>
 * </ul>
 * 
 * @author Aaron Collegeman acollegeman@clutch-inc.com
 * @since JRE 1.5.0
 * @see http://us3.php.net/manual/en/function.strtotime.php
 */
public class StringToTime {

    /**
     * Parse {@code string} and return a {@link DateTime} object.
     * 
     * @param string
     * @return the corresponding DateTime
     */
    public static DateTime parseDateTime(String string) {
        return new DateTime(parseLong(string));
    }

    /**
     * Parse {@code string} and return a timestamp with millisecond precision.
     * 
     * @param string
     * @return the corresponding timestamp
     */
    public static long parseLong(String string) {
        return parseLong(string, new Date());
    }

    /**
     * Parse {@code string} and return a {@link DateTime} object.
     * 
     * @param string
     * @param now
     * @return the corresponding DateTime
     */
    protected static DateTime parseDateTime(String string, Date now) {
        return new DateTime(parseLong(string, now));
    }

    /**
     * Parse {@code string} relative to {@code now} and return a timestamp with
     * millisecond precision.
     * 
     * @param string
     * @param now
     * @return the corresponding timestamp
     */
    protected static long parseLong(String string, Date now) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(string));
        try {
            for (PatternAndFormat paf : known) {
                Matcher m = paf.matches(string);
                if(m.matches()) {
                    Long time = paf.parse(string, now, m);
                    return time;
                }
            }
            throw new ParseException("Unrecognized date/time string '" + string
                    + "'", 0);
        }
        catch (Exception e) { // thrown by various features of the parser
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the parse result.
     * 
     * @param trimmedDateTimeString
     * @param now
     * @return
     * @throws ParseException
     */
    private static ParserResult getParserResult(String trimmedDateTimeString,
            Date now) throws ParseException {
        for (PatternAndFormat paf : known) {
            Matcher m = paf.matches(trimmedDateTimeString);
            if(m.matches()) {
                return new ParserResult(
                        paf.parse(trimmedDateTimeString, now, m), paf.f.type);
            }
        }

        return null;
    }

    // An expression of time (hour)(:(minute))?((:(second))(.(millisecond))?)?(
    // *(am?|pm?))?(RFC 822 time zone|general time zone)?
    private static final String timeExpr = "(\\d{1,2})(:(\\d{1,2}))?(:(\\d{1,2})(\\.(\\d{1,3}))?)?( *(am?|pm?))?( *\\-\\d{4}|[a-z]{3}|[a-z ]+)?";

    /**
     * Patterns and formats recognized by the algorithm; first match wins, so
     * insert most specific patterns first.
     */
    private static final PatternAndFormat[] known = {

            // TODO: ISO 8601 and derivatives

            // just the year
            new PatternAndFormat(Pattern.compile("\\d{4}"), new Format(
                    FormatType.YEAR)),

            // decrement, e.g., -1 day
            new PatternAndFormat(Pattern.compile("\\-( *\\d{1,} +[^ ]+){1,}",
                    Pattern.CASE_INSENSITIVE), new Format(FormatType.DECREMENT)),

            // increment, e.g., +1 day
            new PatternAndFormat(Pattern.compile("\\+?( *\\d{1,} +[^ ]+){1,}",
                    Pattern.CASE_INSENSITIVE), new Format(FormatType.INCREMENT)),

            // e.g., October 26 and Oct 26
            new PatternAndFormat(Pattern.compile("([a-z]+) +(\\d{1,2})",
                    Pattern.CASE_INSENSITIVE), new Format(
                    FormatType.MONTH_AND_DATE)),

            // e.g., 26 October 1981, or 26 Oct 1981, or 26 Oct 81
            new PatternAndFormat(Pattern.compile(
                    "\\d{1,2} +[a-z]+ +(\\d{2}|\\d{4})",
                    Pattern.CASE_INSENSITIVE), new Format("d MMM y")),

            // now or today
            new PatternAndFormat(
                    Pattern.compile(
                            "(midnight|now|today|(this +)?(morning|afternoon|evening)|tonight|noon( +tomorrow)?|tomorrow|tomorrow +(morning|afternoon|evening|night|noon)?|yesterday|yesterday +(morning|afternoon|evening|night)?)",
                            Pattern.CASE_INSENSITIVE), new Format(
                            FormatType.WORD)),

            // time, 24-hour and 12-hour
            new PatternAndFormat(Pattern.compile(timeExpr,
                    Pattern.CASE_INSENSITIVE), new Format(FormatType.TIME)),

            // e.g., October 26, 1981 or Oct 26, 1981
            new PatternAndFormat(Pattern.compile(
                    "[a-z]+ +\\d{1,2} *, *(\\d{2}|\\d{4})",
                    Pattern.CASE_INSENSITIVE), new Format("MMM d, y")),

            // e.g., 10/26/1981 or 10/26/81
            new PatternAndFormat(Pattern.compile("\\d{1,2}/\\d{1,2}/\\d{2,4}"),
                    new Format("M/d/y")),

            // e.g., 10-26-1981 or 10-26-81
            new PatternAndFormat(
                    Pattern.compile("\\d{1,2}\\-\\d{1,2}\\-\\d{2,4}"),
                    new Format("M-d-y")),

            // e.g., 10/26 or 10-26
            new PatternAndFormat(
                    Pattern.compile("(\\d{1,2})(/|\\-)(\\d{1,2})"), new Format(
                            FormatType.MONTH_AND_DATE_WITH_SLASHES)),

            // e.g., 1981/10/26
            new PatternAndFormat(Pattern.compile("\\d{4}/\\d{1,2}/\\d{1,2}"),
                    new Format("y/M/d")),

            // e.g., 1981-10-26
            new PatternAndFormat(
                    Pattern.compile("\\d{4}\\-\\d{1,2}\\-\\d{1,2}"),
                    new Format("y-M-d")),

            // e.g., October or Oct
            new PatternAndFormat(
                    Pattern.compile(
                            "(Jan(uary)?|Feb(ruary)?|Mar(ch)?|Apr(il)?|May|Jun(e)?|Jul(y)?|Aug(ust)?|Sep(tember)?|Oct(ober)?|Nov(ember)?|Dec(ember)?)",
                            Pattern.CASE_INSENSITIVE), new Format(
                            FormatType.MONTH)),

            // e.g., Tuesday or Tue
            new PatternAndFormat(
                    Pattern.compile(
                            "(Sun(day)?|Mon(day)?|Tue(sday)?|Wed(nesday)?|Thu(rsday)?|Fri(day)?|Sat(urday)?)",
                            Pattern.CASE_INSENSITIVE), new Format(
                            FormatType.DAY_OF_WEEK)),

            // next, e.g., next Tuesday
            new PatternAndFormat(Pattern.compile("next +(.*)",
                    Pattern.CASE_INSENSITIVE), new Format(FormatType.NEXT)),

            // last, e.g., last Tuesday
            new PatternAndFormat(Pattern.compile("last +(.*)",
                    Pattern.CASE_INSENSITIVE), new Format(FormatType.LAST)),

            // compound statement
            new PatternAndFormat(Pattern.compile("(.*) +(((\\+|\\-){1}.*)|"
                    + timeExpr + ")$", Pattern.CASE_INSENSITIVE), new Format(
                    FormatType.COMPOUND))

    };

    private static class Format {

        private static Pattern unit = Pattern
                .compile("(\\d{1,}) +(s(ec(ond)?)?|mo(n(th)?)?|(hour|hr?)|d(ay)?|(w(eek)?|wk)|m(in(ute)?)?|(y(ear)?|yr))s?");

        private static Pattern removeExtraSpaces = Pattern.compile(" +");

        private static Map<String, Integer> translateDayOfWeek = new HashMap<String, Integer>();

        static {
            translateDayOfWeek.put("sunday", 1);
            translateDayOfWeek.put("sun", 1);
            translateDayOfWeek.put("monday", 2);
            translateDayOfWeek.put("mon", 2);
            translateDayOfWeek.put("tuesday", 3);
            translateDayOfWeek.put("tue", 3);
            translateDayOfWeek.put("wednesday", 4);
            translateDayOfWeek.put("wed", 4);
            translateDayOfWeek.put("thursday", 5);
            translateDayOfWeek.put("thu", 5);
            translateDayOfWeek.put("friday", 6);
            translateDayOfWeek.put("fri", 6);
            translateDayOfWeek.put("saturday", 7);
            translateDayOfWeek.put("sat", 7);
        }

        private String sdf;

        private FormatType type;

        public Format(FormatType type) {
            this.type = type;
        }

        public Format(String sdf) {
            this.sdf = sdf;
        }

        public Date parse(String dateTimeString, Date now, Matcher m)
                throws ParseException {
            if(sdf != null)
                return new SimpleDateFormat(sdf).parse(dateTimeString);
            else {
                dateTimeString = removeExtraSpaces.matcher(dateTimeString)
                        .replaceAll(" ").toLowerCase();

                try {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(now);

                    // word expressions, e.g., "now" and "today" and "tonight"
                    if(type == FormatType.WORD) {
                        if("now".equalsIgnoreCase(dateTimeString))
                            return (now != null ? now : new Date());

                        else if("today".equalsIgnoreCase(dateTimeString)) {
                            cal.set(Calendar.HOUR_OF_DAY, 0);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
                            return new Date(cal.getTimeInMillis());
                        }

                        else if("morning".equalsIgnoreCase(dateTimeString)
                                || "this morning"
                                        .equalsIgnoreCase(dateTimeString)) {
                            // by default, this morning begins at 07:00:00.000
                            int thisMorningBeginsAt = Integer.parseInt(System
                                    .getProperty(StringToTime.class
                                            + ".THIS_MORNING_BEGINS_AT", "7"));
                            cal.set(Calendar.HOUR_OF_DAY, thisMorningBeginsAt);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
                            return new Date(cal.getTimeInMillis());
                        }

                        else if("noon".equalsIgnoreCase(dateTimeString)) {
                            // noon is 12:00:00.000
                            cal.set(Calendar.HOUR_OF_DAY, 12);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
                            return new Date(cal.getTimeInMillis());
                        }

                        else if("afternoon".equalsIgnoreCase(dateTimeString)
                                || "this afternoon"
                                        .equalsIgnoreCase(dateTimeString)) {
                            // by default, this afternoon begins at 13:00:00.000
                            int thisAfternoonBeginsAt = Integer
                                    .parseInt(System
                                            .getProperty(
                                                    StringToTime.class
                                                            + ".THIS_AFTERNOON_BEGINS_AT",
                                                    "13"));
                            cal.set(Calendar.HOUR_OF_DAY, thisAfternoonBeginsAt);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
                            return new Date(cal.getTimeInMillis());
                        }

                        else if("evening".equalsIgnoreCase(dateTimeString)
                                || "this evening"
                                        .equalsIgnoreCase(dateTimeString)) {
                            // by default, this evening begins at 17:00:00.000
                            int thisEveningBeginsAt = Integer.parseInt(System
                                    .getProperty(StringToTime.class
                                            + ".THIS_EVENING_BEGINS_AT", "17"));
                            cal.set(Calendar.HOUR_OF_DAY, thisEveningBeginsAt);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
                            return new Date(cal.getTimeInMillis());
                        }

                        else if("tonight".equalsIgnoreCase(dateTimeString)) {
                            // by default, tonight begins at 20:00:00.000
                            int tonightBeginsAt = Integer.parseInt(System
                                    .getProperty(StringToTime.class
                                            + ".TONIGHT_BEGINS_AT", "20"));
                            cal.set(Calendar.HOUR_OF_DAY, tonightBeginsAt);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
                            return new Date(cal.getTimeInMillis());
                        }

                        else if("midnight".equalsIgnoreCase(dateTimeString)) {
                            return new Date(
                                    parseLong("00:00:00 +24 hours", now));
                        }

                        else if("tomorrow".equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("now +24 hours", now));
                        }

                        else if("tomorrow morning"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("morning +24 hours", now));
                        }

                        else if("tomorrow noon"
                                .equalsIgnoreCase(dateTimeString)
                                || "noon tomorrow"
                                        .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("noon +24 hours", now));
                        }

                        else if("tomorrow afternoon"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("afternoon +24 hours",
                                    now));
                        }

                        else if("tomorrow evening"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("evening +24 hours", now));
                        }

                        else if("tomorrow night"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("tonight +24 hours", now));
                        }

                        else if("yesterday".equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("now -24 hours", now));
                        }

                        else if("yesterday morning"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("morning -24 hours", now));
                        }

                        else if("yesterday noon"
                                .equalsIgnoreCase(dateTimeString)
                                || "noon yesterday"
                                        .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("noon -24 hours", now));
                        }

                        else if("yesterday afternoon"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("afternoon -24 hours",
                                    now));
                        }

                        else if("yesterday evening"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("evening -24 hours", now));
                        }

                        else if("yesterday night"
                                .equalsIgnoreCase(dateTimeString)) {
                            return new Date(parseLong("tonight -24 hours", now));
                        }

                        else
                            throw new ParseException(String.format(
                                    "Unrecognized date word: %s",
                                    dateTimeString), 0);
                    }

                    // time expressions, 24-hour and 12-hour
                    else if(type == FormatType.TIME) {
                        // An expression of time
                        // (hour)(:(minute))?((:(second))(.(millisecond))?)?(
                        // *(am?|pm?))?(RFC 822 time zone|general time zone)?
                        String hour = m.group(1);
                        String min = m.group(3);
                        String sec = m.group(5);
                        String ms = m.group(7);
                        String amOrPm = m.group(8);

                        if(hour != null) {
                            if(amOrPm != null)
                                cal.set(Calendar.HOUR, new Integer(hour));
                            else
                                cal.set(Calendar.HOUR_OF_DAY, new Integer(hour));
                        }
                        else
                            cal.set(Calendar.HOUR, 0);

                        cal.set(Calendar.MINUTE,
                                (min != null ? new Integer(min) : 0));
                        cal.set(Calendar.SECOND,
                                (sec != null ? new Integer(sec) : 0));
                        cal.set(Calendar.MILLISECOND,
                                (ms != null ? new Integer(ms) : 0));

                        if(amOrPm != null)
                            cal.set(Calendar.AM_PM,
                                    (amOrPm.equalsIgnoreCase("a")
                                            || amOrPm.equalsIgnoreCase("am") ? Calendar.AM
                                            : Calendar.PM));

                        return new Date(cal.getTimeInMillis());
                    }

                    // increments
                    else if(type == FormatType.INCREMENT
                            || type == FormatType.DECREMENT) {
                        Matcher units = unit.matcher(dateTimeString);
                        while (units.find()) {
                            Integer val = new Integer(units.group(1))
                                    * (type == FormatType.DECREMENT ? -1 : 1);
                            String u = units.group(2);

                            // second
                            if("s".equalsIgnoreCase(u)
                                    || "sec".equalsIgnoreCase(u)
                                    || "second".equalsIgnoreCase(u))
                                cal.set(Calendar.SECOND,
                                        cal.get(Calendar.SECOND) + val);

                            // minute
                            else if("m".equalsIgnoreCase(u)
                                    || "min".equalsIgnoreCase(u)
                                    || "minute".equalsIgnoreCase(u))
                                cal.set(Calendar.MINUTE,
                                        cal.get(Calendar.MINUTE) + val);

                            // hour
                            else if("h".equalsIgnoreCase(u)
                                    || "hr".equalsIgnoreCase(u)
                                    || "hour".equalsIgnoreCase(u))
                                cal.set(Calendar.HOUR_OF_DAY,
                                        cal.get(Calendar.HOUR_OF_DAY) + val);

                            // day
                            else if("d".equalsIgnoreCase(u)
                                    || "day".equalsIgnoreCase(u))
                                cal.set(Calendar.DATE, cal.get(Calendar.DATE)
                                        + val);

                            // week
                            else if("w".equalsIgnoreCase(u)
                                    || "wk".equalsIgnoreCase(u)
                                    || "week".equalsIgnoreCase(u))
                                cal.set(Calendar.WEEK_OF_YEAR,
                                        cal.get(Calendar.WEEK_OF_YEAR) + val);

                            // month
                            else if("mo".equalsIgnoreCase(u)
                                    || "mon".equalsIgnoreCase(u)
                                    || "month".equalsIgnoreCase(u))
                                cal.set(Calendar.MONTH, cal.get(Calendar.MONTH)
                                        + val);

                            // year
                            else if("y".equalsIgnoreCase(u)
                                    || "yr".equalsIgnoreCase(u)
                                    || "year".equalsIgnoreCase(u))
                                cal.set(Calendar.YEAR, cal.get(Calendar.YEAR)
                                        + val);

                            else
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Unrecognized %s unit: [%s]",
                                                type, u));
                        }

                        return new Date(cal.getTimeInMillis());
                    }

                    // compound expressions
                    else if(type == FormatType.COMPOUND) {
                        return new Date(parseLong(m.group(2), new Date(
                                parseLong(m.group(1), now))));
                    }

                    // month of the year
                    else if(type == FormatType.MONTH) {
                        Calendar ref = Calendar.getInstance();
                        ref.setTime(new SimpleDateFormat("MMM d, y")
                                .parse(String.format("%s 1, 1970", m.group(1))));
                        cal.set(Calendar.MONTH, ref.get(Calendar.MONTH));

                        return new Date(cal.getTimeInMillis());
                    }

                    // day of week
                    else if(type == FormatType.DAY_OF_WEEK) {
                        Integer ref = translateDayOfWeek.get(dateTimeString);
                        if(cal.get(Calendar.DAY_OF_WEEK) >= ref)
                            cal.set(Calendar.WEEK_OF_YEAR,
                                    cal.get(Calendar.WEEK_OF_YEAR) + 1);
                        cal.set(Calendar.DAY_OF_WEEK, ref);

                        return new Date(cal.getTimeInMillis());
                    }

                    // month and day with slashes
                    else if(type == FormatType.MONTH_AND_DATE_WITH_SLASHES) {
                        Calendar ref = Calendar.getInstance();
                        ref.setTime(new SimpleDateFormat("M/d/y").parse(String
                                .format("%s/%s/1970", m.group(1), m.group(3))));
                        cal.set(Calendar.MONTH, ref.get(Calendar.MONTH));
                        cal.set(Calendar.DATE, ref.get(Calendar.DATE));

                        return new Date(cal.getTimeInMillis());
                    }

                    // month and day long-hand
                    else if(type == FormatType.MONTH_AND_DATE) {
                        Calendar ref = Calendar.getInstance();
                        ref.setTime(new SimpleDateFormat("MMM d, y")
                                .parse(String.format("%s %s, 1970", m.group(1),
                                        m.group(2))));
                        cal.set(Calendar.MONTH, ref.get(Calendar.MONTH));
                        cal.set(Calendar.DATE, ref.get(Calendar.DATE));

                        return new Date(cal.getTimeInMillis());
                    }

                    // next X
                    else if(type == FormatType.NEXT) {
                        // Format types MONTH and DAY_OF_WEEK both return future
                        // dates, so no additional processing is needed
                        String expr = m.group(1);
                        ParserResult parsed = StringToTime.getParserResult(
                                expr, now);

                        if(parsed != null
                                && (FormatType.MONTH.equals(parsed.type)
                                        || FormatType.DAY_OF_WEEK
                                                .equals(parsed.type) || FormatType.MONTH_AND_DATE
                                            .equals(parsed.type)))
                            return new Date(parsed.timestamp);
                        else {
                            if("week".equalsIgnoreCase(expr))
                                cal.set(Calendar.WEEK_OF_YEAR,
                                        cal.get(Calendar.WEEK_OF_YEAR) + 1);
                            else if("month".equalsIgnoreCase(expr))
                                cal.set(Calendar.MONTH,
                                        cal.get(Calendar.MONTH) + 1);
                            else if("year".equalsIgnoreCase(expr))
                                cal.set(Calendar.YEAR,
                                        cal.get(Calendar.YEAR) + 1);
                            else
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid expression of time: %s",
                                                dateTimeString));

                            return new Date(cal.getTimeInMillis());
                        }
                    }

                    // last X
                    else if(type == FormatType.LAST) {
                        String expr = m.group(1);
                        ParserResult parsed = StringToTime.getParserResult(
                                expr, now);

                        if(parsed != null
                                && (FormatType.MONTH.equals(parsed.type) || FormatType.MONTH_AND_DATE
                                        .equals(parsed.type))) {
                            return new Date(parseLong("-1 year", new Date(
                                    parsed.timestamp)));
                        }

                        else if(parsed != null
                                && FormatType.DAY_OF_WEEK.equals(parsed.type)) {
                            return new Date(parseLong("-1 week", new Date(
                                    parsed.timestamp)));
                        }

                        else {
                            if("week".equalsIgnoreCase(expr))
                                cal.set(Calendar.WEEK_OF_YEAR,
                                        cal.get(Calendar.WEEK_OF_YEAR) - 1);
                            else if("month".equalsIgnoreCase(expr))
                                cal.set(Calendar.MONTH,
                                        cal.get(Calendar.MONTH) - 1);
                            else if("year".equalsIgnoreCase(expr))
                                cal.set(Calendar.YEAR,
                                        cal.get(Calendar.YEAR) - 1);
                            else
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Invalid expression of time: %s",
                                                dateTimeString));

                            return new Date(cal.getTimeInMillis());
                        }
                    }

                    // year
                    else if(type == FormatType.YEAR) {
                        cal.set(Calendar.YEAR, new Integer(m.group(0)));
                        return new Date(cal.getTimeInMillis());
                    }

                    // unimplemented format type
                    else
                        throw new IllegalStateException(String.format(
                                "Unimplemented FormatType: %s", type));
                }
                catch (ParseException e) {
                    throw e;
                }
                catch (IllegalStateException e) {
                    throw e;
                }
                catch (IllegalArgumentException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new RuntimeException(String.format(
                            "Unknown failure in string-to-time conversion: %s",
                            e.getMessage()), e);
                }
            }
        }

        @Override
        public String toString() {
            if(sdf != null)
                return sdf;
            else
                return type.toString();
        }
    }

    private enum FormatType {
        COMPOUND,
        MONTH_AND_DATE_WITH_SLASHES,
        MONTH_AND_DATE,
        MONTH,
        DAY_OF_WEEK,
        NEXT,
        LAST,
        INCREMENT,
        DECREMENT,
        WORD,
        TIME,
        YEAR
    }

    private static class ParserResult {
        public FormatType type;
        public Long timestamp;

        public ParserResult(Long timestamp, FormatType type) {
            this.timestamp = timestamp;
            this.type = type;
        }
    }

    private static class PatternAndFormat {
        public Pattern p;
        public Format f;

        public PatternAndFormat(Pattern p, Format f) {
            this.p = p;
            this.f = f;
        }

        public Matcher matches(String dateTimeString) {
            return p.matcher(dateTimeString);
        }

        public Long parse(String dateTimeString, Date now, Matcher m)
                throws ParseException {
            return f.parse(dateTimeString, now, m).getTime();
        }
    }

}