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

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.slf4j.helpers.MessageFormatter;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.gson.JsonParseException;

/**
 * Yet another collection of utility functions for Strings that tries to add new
 * functionality or optimize existing ones.
 * 
 * @author Jeff Nelson
 */
public final class Strings {

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
        return isWithinQuotes(string) ? string : joinSimple("\"", string, "\"");
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
        char c = '\0';
        StringBuilder sb = null;
        Set<Character> chars = null;
        if(characters.length == 1) {
            c = characters[0];
        }
        else {
            chars = Sets.newHashSetWithExpectedSize(characters.length);
            for (char ch : characters) {
                chars.add(ch);
            }
        }
        char[] schars = string.toCharArray();
        int offset = 0;
        int i = 0;
        while (i < schars.length) {
            if(i > 0 && i < schars.length - 1) {
                char schar = schars[i];
                if((c != '\0' && c == schar)
                        || (chars != null && chars.contains(schar))) {
                    sb = Objects.firstNonNull(sb, new StringBuilder());
                    sb.append(schars, offset, i - offset);
                    sb.append('\\');
                    sb.append(schar);
                    offset = i + 1;
                }
            }
            ++i;
        }
        if(sb != null) {
            sb.append(schars, offset, i - offset);
            return sb.toString();
        }
        else {
            return string;
        }
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
        return MessageFormatter.arrayFormat(pattern, params).getMessage();
    }

    /**
     * Return a set that contains every possible substring of {@code string}
     * excluding pure whitespace strings.
     * 
     * @param string the string to divide into substrings
     * @return the set of substrings
     */
    public static Set<String> getAllSubStrings(String string) {
        Set<String> result = Sets.newHashSet();
        for (int i = 0; i < string.length(); ++i) {
            for (int j = i + 1; j <= string.length(); ++j) {
                String substring = string.substring(i, j).trim();
                if(!com.google.common.base.Strings.isNullOrEmpty(substring)) {
                    result.add(substring);
                }
            }
        }
        return result;
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
        if(needle.length() > haystack.length()) {
            return false;
        }
        else if(needle.length() == haystack.length()) {
            return needle.equals(haystack);
        }
        else {
            char[] n = needle.toCharArray();
            char[] h = haystack.toCharArray();
            int npos = 0;
            int hpos = 0;
            int stop = h.length - n.length;
            int hstart = -1;
            while (hpos < h.length && npos < n.length) {
                char hi = h[hpos];
                char ni = n[npos];
                if(hi == ni) {
                    if(hstart == -1) {
                        hstart = hpos;
                    }
                    ++npos;
                    ++hpos;
                }
                else {
                    if(npos > 0) {
                        npos = 0;
                        hpos = hstart + 1;
                        hstart = -1;
                    }
                    else {
                        ++hpos;
                    }
                    if(hpos > stop) {
                        return false;
                    }
                }
            }
            return npos == n.length;
        }
    }

    /**
     * Return {@code true} if the {@code json} string is valid, otherwise return
     * {@code false}.
     * 
     * @param json a json formatted string
     * @return {@code true} if the {@code json} is valid
     */
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
     * Concatenates the {@link Object#toString string} representation of all the
     * {@code args}, separated by the {@code separator} char in an efficient
     * manner.
     * 
     * @param separator the separator to place between each of the {@code args}
     * @param args the args to join
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
     * Concatenates the {@link Object#toString string} representation of all the
     * {@code args}, separated by the {@code separator} string in an efficient
     * manner.
     * 
     * @param separator the separator to place between each of the {@code args}
     * @param args the args to join
     * @return the resulting String
     */
    public static String join(String separator, Object... args) {
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
     * @deprecated in version 0.5.0, use {@link QuoteAwareStringSplitter}
     *             instead.
     */
    @Deprecated
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
     * @return a list of tokens after splitting the string on camel case word
     *         boundaries
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
        int size = value.length();
        if(value == null || size == 0) {
            return null;
        }
        else if(value.charAt(0) == '0' && size > 1 && value.charAt(1) != '.') {
            // Do not parse a string as a number if it has a leading 0 that is
            // not followed by a decimal (i.e. 007)
            return null;
        }
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
            if(decimal) {
                // Try to return a float (for space compactness) if it is
                // possible to fit the entire decimal without any loss of
                // precision. In order to do this, we have to compare the string
                // output of both the parsed double and the parsed float. This
                // is kind of inefficient, so substitute for a better way if it
                // exists.
                double d = Doubles.tryParse(value);
                float f = Floats.tryParse(value);
                if(String.valueOf(d).equals(String.valueOf(f))) {
                    return f;
                }
                else {
                    return d;
                }
            }
            else {
                return Objects.firstNonNull(Ints.tryParse(value),
                        Longs.tryParse(value));
            }
        }
        catch (NullPointerException e) {
            throw new NumberFormatException(
                    Strings.format(
                            "{} appears to be a number cannot be parsed as such",
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

    /**
     * Similar to the {@link String#valueOf(char)} method, but this one will
     * return a cached copy of the string for frequently used characters.
     * 
     * @param c the character to convert
     * @return a string of length 1 containing the input char
     */
    public static String valueOfCached(char c) {
        if(c == '(') {
            return "(";
        }
        else if(c == ')') {
            return ")";
        }
        else {
            return String.valueOf(c);
        }
    }

    private Strings() {/* noop */}

}
