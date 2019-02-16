/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.common.base.AnyStrings;
import com.google.gson.JsonParseException;

/**
 * Yet another collection of utility functions for Strings that tries to add new
 * functionality or optimize existing ones.
 * 
 * @author Jeff Nelson
 * @deprecated in version 0.9.6; use {@link AnyStrings} instead
 */
@Deprecated
public final class Strings {

    /**
     * Ensure that {@code string} ends with {@code suffix} by appending it to
     * {@code string} if and only if it is not already the last sequence of
     * characters in the string.
     * 
     * @param string the {@link String} to that should end with {@code suffix}
     * @param suffix the {@link String} of characters with which {@code string}
     *            should end
     * @return {@code string} if it already ends with {@code suffix} or a new
     *         {@link String} that contains {@code suffix} appended to
     *         {@code string}
     */
    public static String ensureEndsWith(String string, String suffix) {
        return AnyStrings.ensureEndsWith(string, suffix);
    }

    /**
     * Ensure that {@code string} starts with {@code prefix} by prepending it to
     * {@code string} if and only if it is not already the first sequence of
     * characters in the string.
     * 
     * @param string the {@link String} to that should start with {@code prefix}
     * @param prefix the {@link String} of characters with which {@code string}
     *            should start
     * @return {@code string} if it already begins with {@code prefix} or a new
     *         {@link String} that contains {@code prefix} prepended to
     *         {@code string}
     */
    public static String ensureStartsWith(String string, String prefix) {
        return AnyStrings.ensureStartsWith(string, prefix);
    }

    /**
     * Ensure that {@code string} is surrounded by quotes. If that is not the
     * case, alter the string so that it is and return the altered form.
     * 
     * <p>
     * Calling {@link Strings#isWithinQuotes(String)} on the result of this
     * method will always return {@code true}.
     * </p>
     * 
     * @param string the string that must be quoted
     * @return {@code string} or {@code string} surrounded by quotes if it is
     *         not already
     */
    public static String ensureWithinQuotes(String string) {
        return AnyStrings.ensureWithinQuotes(string);
    }

    /**
     * Wrap {@code string} within quotes if it is necessary to do so. Otherwise,
     * return the original {@code string}.
     * 
     * <p>
     * The original {@code string} will be wrapped in quotes and returned as
     * such if:
     * <ul>
     * <li>it is not already wrapped {@link #isWithinQuotes(String) within
     * quotes}, and</li>
     * <li>{@code delimiter} appears at least once</li>
     * </ul>
     * If those conditions are met, the original string will be wrapped in
     * either
     * <ul>
     * <li>double quotes if a single quote appears in the original string,
     * or</li>
     * <li>single quotes if a double quote appears in the original string,
     * or</li>
     * <li>double quotes if both a single and double quote appear in the
     * original string; furthermore, all instances of double quotes within the
     * original string will be escaped</li>
     * </ul>
     * </p>
     * 
     * @param string the string to potentially quote
     * @param delimiter the delimiter that determines whether quoting should
     *            happen
     * @return the original {@code string} or a properly quoted alternative
     */
    public static String ensureWithinQuotesIfNeeded(String string,
            char delimiter) {
        return AnyStrings.ensureWithinQuotesIfNeeded(string, delimiter);
    }

    /**
     * Efficiently escape inner occurrences of each of the {@code characters}
     * within the {@code string}, if necessary.
     * <p>
     * Escaped characters are prepended with the backslash ('\') character.
     * </p>
     * <p>
     * An "inner occurrence" for a character is one that is not at the head or
     * tail of the string.
     * </p>
     * 
     * @param string the string to escape
     * @param characters the characters to escape within the {@code string}
     * @return the escaped {@code string}
     */
    public static String escapeInner(String string, char... characters) {
        return AnyStrings.escapeInner(string, characters);
    }

    /**
     * Replace all instances of "confusable" unicode characters with a
     * canoncial/normalized character.
     * <p>
     * See <a href=
     * "http://www.unicode.org/Public/security/revision-03/confusablesSummary.txt"
     * >http://www.unicode.org/Public/security/revision-03/confusablesSummary.
     * txt</a> for a list of characters that are considered to be confusable.
     * </p>
     * 
     * @param string the {@link String} in which the replacements should occur
     * @return a {@link String} free of confusable unicode characters
     */
    public static String replaceUnicodeConfusables(String string) {
        return AnyStrings.replaceUnicodeConfusables(string);
    }

    /**
     * Perform string substitution and formatting in a manner that is similar to
     * the SLF4J library.
     * 
     * <pre>
     * Strings#format("Bob is very {} because he has no {}", "brave", "fear") = "Bob is very brave because he has no fear"
     * </pre>
     * <p>
     * <strong>NOTE:</strong> This method is less efficient than using a
     * {@link StringBuilder} and manually appending variable arguments for
     * interpolation. This is provided for convenience, but don't use it for
     * anything that is performance critical.
     * </p>
     * 
     * @param pattern the message pattern which will be parsed and formatted
     * @param params an array of arguments to be substituted in place of
     *            formatting anchors
     * @return The formatted message
     */
    public static String format(String pattern, Object... params) {
        return AnyStrings.format(pattern, params);
    }

    /**
     * Return a set that contains every possible substring of {@code string}
     * excluding pure whitespace strings.
     * 
     * @param string the string to divide into substrings
     * @return the set of substrings
     */
    public static Set<String> getAllSubStrings(String string) {
        return AnyStrings.getAllSubStrings(string);
    }

    /**
     * An optimized version of {@link String#contains(CharSequence)} to see if
     * {@code needle} is a substring of {@code haystack}.
     * 
     * @param needle the substring for which to search
     * @param haystack the string in which to search for the substring
     * @return {@code true} if {@code needle} is a substring
     */
    public static boolean isSubString(String needle, String haystack) {
        return AnyStrings.isSubString(needle, haystack);
    }

    /**
     * Return {@code true} if the {@code json} string is valid, otherwise return
     * {@code false}.
     * 
     * @param json a json formatted string
     * @return {@code true} if the {@code json} is valid
     * @deprecated in version 0.9.6; use
     *             {@link com.cinchapi.concourse.util.DataServices#jsonParser()#parse()}
     */
    @Deprecated
    public static boolean isValidJson(String json) {
        char first = json.charAt(0);
        char last = json.charAt(json.length() - 1);
        if((first == '[' || first == '{') && (last == ']' || last == '}')) {
            try {
                DataServices.jsonParser().parse(json);
                return true;
            }
            catch (JsonParseException e) {
                return false;
            }
        }
        else {
            return false;
        }
    }

    /**
     * Return {@code true} if {@code string} both starts and ends with single or
     * double quotes.
     * 
     * @param string
     * @return {@code true} if the string is between quotes
     */
    public static boolean isWithinQuotes(String string) {
        return AnyStrings.isWithinQuotes(string);
    }

    /**
     * Concatenates the {@link Object#toString string} representation of all the
     * {@code args}, separated by the {@code separator} char in an efficient
     * manner.
     * 
     * @param separator the separator to place between each of the {@code args}
     * @param args the args to join
     * @return the resulting String
     */
    public static String join(char separator, Object... args) {
        return AnyStrings.join(separator, args);
    }

    /**
     * Concatenates the {@link Object#toString string} representation of all the
     * {@code args}, separated by the {@code separator} string in an efficient
     * manner.
     * 
     * @param separator the separator to place between each of the {@code args}
     * @param args the args to join
     * @return the resulting String
     */
    public static String join(String separator, Object... args) {
        return AnyStrings.join(separator, args);
    }

    /**
     * Concatenates the toString values of all the {@code args} in an efficient
     * manner.
     * 
     * @param args
     * @return the resulting String
     */
    public static String joinSimple(Object... args) {
        return AnyStrings.joinSimple(args);
    }

    /**
     * Concatenates the toString values of all the {@code args}, separated by
     * whitespace in an efficient manner.
     * 
     * @param args
     * @return the resulting String
     */
    public static String joinWithSpace(Object... args) {
        return AnyStrings.joinWithSpace(args);
    }

    /**
     * Split a string, using whitespace as a delimiter, as long as the
     * whitespace is not wrapped in double or single quotes.
     * 
     * @param string
     * @return the tokens that result from the split
     * @deprecated in version 0.5.0, use {@link QuoteAwareStringSplitter}
     *             instead.
     */
    @Deprecated
    public static String[] splitButRespectQuotes(String string) {
        return AnyStrings.splitButRespectQuotes(string);
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
     * @return a list of tokens after splitting the string on camel case word
     *         boundaries
     */
    public static List<String> splitCamelCase(String string) {
        return AnyStrings.splitCamelCase(string);
    }

    /**
     * Split a string on a delimiter as long as that delimiter is not wrapped in
     * double or single quotes.
     * <p>
     * If {@code delimiter} is a single character string, it is more efficient
     * to use a {@link StringSplitter} as opposed to this method.
     * </p>
     * 
     * @param string the string to split
     * @param delimiter the delimiting string/regex on which the input
     *            {@code string} is split
     * @return the tokens that result from the split
     */
    public static String[] splitStringByDelimiterButRespectQuotes(String string,
            String delimiter) {
        return AnyStrings.splitStringByDelimiterButRespectQuotes(string,
                delimiter);
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
        return AnyStrings.tryParseBoolean(value);
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
        return AnyStrings.tryParseNumber(value);
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
        return AnyStrings.tryParseNumberStrict(value);
    }

    /**
     * Similar to the {@link String#valueOf(char)} method, but this one will
     * return a cached copy of the string for frequently used characters.
     * 
     * @param c the character to convert
     * @return a string of length 1 containing the input char
     */
    public static String valueOfCached(char c) {
        return AnyStrings.valueOfCached(c);
    }

    private Strings() {/* noop */}

}