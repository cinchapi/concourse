/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.text.MessageFormat;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Yet another collection of utility functions for Strings.
 * 
 * @author Jeff Nelson
 */
public final class Strings {

    /**
     * Return {@code true} if {@code string} both starts and ends with single or
     * double quotes.
     * 
     * @param string
     * @return {@code true} if the string is between quotes
     */
    public static boolean isWithinQuotes(String string) {
        if(string.length() >= 2) {
            char first = string.charAt(0);
            if(first == '"' || first == '\'') {
                char last = string.charAt(string.length() - 1);
                return first == last;
            }
        }
        return false;
    }

    /**
     * Concatenates the toString values of all the {@code args}, separated by
     * the {@code separator} char in an efficient manner.
     * 
     * @param separator
     * @param args
     * @return the resulting String
     */
    public static String join(char separator, Object... args) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < args.length; ++i) {
            builder.append(args[i]);
            builder.append(separator);
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    /**
     * Concatenates the toString values of all the {@code args} in an efficient
     * manner.
     * 
     * @param args
     * @return the resulting String
     */
    public static String joinSimple(Object... args) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < args.length; ++i) {
            builder.append(args[i]);
        }
        return builder.toString();
    }

    /**
     * Concatenates the toString values of all the {@code args}, separated by
     * whitespace in an efficient manner.
     * 
     * @param separator
     * @param args
     * @return the resulting String
     */
    public static String joinWithSpace(Object... args) {
        return join(' ', args);
    }

    /**
     * Split a string, using whitespace as a delimiter, as long as the
     * whitespace is not wrapped in double or single quotes.
     * 
     * @param string
     * @return the tokens that result from the split
     */
    public static String[] splitButRespectQuotes(String string) {
        return splitStringByDelimiterButRespectQuotes(string, " ");
    }

    /**
     * Split a camel case {@code string} into tokens that represent the distinct
     * words.
     * <p>
     * <h1>Example</h1>
     * <ul>
     * thisIsACamelCaseSTRING -> [this, Is, A, Camel, Case, S, T, R, I, N, G]
     * </ul>
     * <ul>
     * ThisIsACamelCaseSTRING -> [This, Is, A, Camel, Case, S, T, R, I, N, G]
     * </ul>
     * <ul>
     * thisisacamelcasestring -> [thisisacamelcasestring]
     * </ul>
     * </p>
     * 
     * @param string
     * @return
     */
    public static List<String> splitCamelCase(String string) {
        List<String> words = Lists.newArrayList();
        char[] chars = string.toCharArray();
        StringBuilder word = new StringBuilder();
        for (int i = 0; i < chars.length; ++i) {
            char c = chars[i];
            if(Character.isUpperCase(c) || c == '$') {
                if(word.length() > 0) {
                    words.add(word.toString());
                }
                word.setLength(0);
            }
            word.append(c);
        }
        words.add(word.toString());
        return words;
    }

    /**
     * Split a string on a delimiter as long as that delimiter is not wrapped in
     * double or single quotes.
     * 
     * @param string
     * @param delimiter
     * @return the tokens that result from the split
     */
    public static String[] splitStringByDelimiterButRespectQuotes(
            String string, String delimiter) {
        // This is pretty inefficient: convert all single quotes to double
        // quotes (except one off single quotes that are used as apostrophes) so
        // the regex below works
        string = string.replaceAll(" '", " \"");
        string = string.replaceAll("' ", "\" ");
        string = string.replaceAll("'$", "\"");
        return string.split(delimiter + "(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    /**
     * This method efficiently tries to parse {@code value} into a
     * {@link Boolean} object if possible. If the string is not a boolean, then
     * the method returns {@code null} as quickly as possible.
     * 
     * @param value
     * @return a Boolean object that represents the string or {@code null} if it
     *         is not possible to parse the string into a boolean
     */
    public static Boolean tryParseBoolean(String value) {
        if(value.equalsIgnoreCase("true")) {
            return true;
        }
        else if(value.equalsIgnoreCase("false")) {
            return false;
        }
        else {
            return null;
        }
    }

    /**
     * This method efficiently tries to parse {@code value} into a
     * {@link Number} object if possible. If the string is not a number, then
     * the method returns {@code null} as quickly as possible.
     * 
     * @param value
     * @return a Number object that represents the string or {@code null} if it
     *         is not possible to parse the string into a number
     */
    @Nullable
    public static Number tryParseNumber(String value) {
        if(value == null || value.length() == 0) {
            return null;
        }
        int size = value.length();
        boolean decimal = false;
        for (int i = 0; i < size; ++i) {
            char c = value.charAt(i);
            if(!Character.isDigit(c)) {
                if(i == 0 && c == '-') {
                    continue;
                }
                else if(c == '.') {
                    decimal = true;
                }
                else if(i == size - 1 && c == 'D' && size > 1) {
                    // Respect the convention to coerce numeric strings to
                    // Double objects by appending a single 'D' character.
                    return Double.valueOf(value.substring(0, i));
                }
                else {
                    return null;
                }
            }
        }
        try {
            return decimal ? Objects.firstNonNull(Floats.tryParse(value),
                    Doubles.tryParse(value)) : Objects.firstNonNull(
                    Ints.tryParse(value), Longs.tryParse(value));
        }
        catch (NullPointerException e) {
            throw new NumberFormatException(MessageFormat.format(
                    "{0} appears to be a number cannot be parsed as such",
                    value));
        }
    }

    /**
     * A stricter version of {@link #tryParseNumber(String)} that does not parse
     * strings that masquerade as numbers (i.e. 3.124D). Instead this method
     * will only parse the string into a Number if it contains characters that
     * are either a decimal digit, a decimal separator or a negative sign.
     * 
     * @param value
     * @return a Number object that represents the string or {@code null} if it
     *         is not possible to parse the string into a number
     */
    @Nullable
    public static Number tryParseNumberStrict(String value) {
        if(value == null || value.length() == 0) {
            return null;
        }
        char last = value.charAt(value.length() - 1);
        if(Character.isDigit(last)) {
            return tryParseNumber(value);
        }
        else {
            return null;
        }
    }

    private Strings() {/* noop */}

}
