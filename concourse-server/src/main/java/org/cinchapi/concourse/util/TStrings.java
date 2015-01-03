/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cinchapi.concourse.server.GlobalState;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

/**
 * A collection of {@link String} related tools.
 * 
 * @author jnelson
 */
public final class TStrings {

    /**
     * Return a set that contains every possible substring of {@code string}
     * excluding pure whitespace strings.
     * 
     * @param string
     * @return the set of substrings
     */
    public static Set<String> getAllSubStrings(String string) {
        Set<String> result = Sets.newHashSet();
        for (int i = 0; i < string.length(); ++i) {
            for (int j = i + 1; j <= string.length(); ++j) {
                String substring = string.substring(i, j).trim();
                if(!Strings.isNullOrEmpty(substring)) {
                    result.add(substring);
                }
            }
        }
        return result;
    }

    /**
     * Return {@code true} if {@code haystack} is an <strong>infix
     * search match</strong> for {@code needle}. If {@code haystack} is an infix
     * search match, it means that it contains a sequence of terms where each
     * term or a substring of the term matches the term in the same relative
     * position in {@code needle}.
     * <p>
     * <ul>
     * <li><em>foo bar</em> (haystack) <strong>IS</strong> a match for
     * <em>foo bar</em> (needle)</li>
     * <li><em>foo bar</em> (haystack) <strong>IS</strong> a match for
     * <em>f bar</em> (needle)</li>
     * <li><em>foo bar</em> (haystack) <strong>IS</strong> a match for
     * <em>oo a</em> (needle)</li>
     * <li><em>f b</em> (haystack) <strong>IS</strong> a match for
     * <em>f bar</em> (needle)</li>
     * <li><em>barfoobar foobarfoo</em> (haystack) <strong>IS</strong> a match
     * for <em>f bar</em> (needle)</li>
     * </ul>
     * </p>
     * 
     * @param needle
     * @param haystack
     * @return {@code true} if {@code haystack} is an infix search match for
     *         {@code needle}.
     */
    public static boolean isInfixSearchMatch(String needle, String haystack) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String term : needle
                .split(REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS)) {
            if(!first) {
                sb.append(REGEX_SINGLE_WHITESPACE);
            }
            sb.append(REGEX_ZERO_OR_MORE_NON_WHITESPACE_CHARS);
            sb.append(term);
            sb.append(REGEX_ZERO_OR_MORE_NON_WHITESPACE_CHARS);
            first = false;
        }
        Pattern pattern = Pattern.compile(sb.toString());
        Matcher matcher = pattern.matcher(haystack);
        return matcher.find();
    }

    /**
     * Return a copy of {@code string} with all of the stopwords removed. This
     * method depends on the stopwords defined in {@link GlobalState#STOPWORDS}.
     * 
     * @param string
     * @return A copy of {@code string} without stopwords
     */
    public static String stripStopWords(String string) {
        String[] toks = string
                .split(REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
        StringBuilder sb = new StringBuilder();
        for (String tok : toks) {
            if(!GlobalState.STOPWORDS.contains(tok)) {
                sb.append(tok);
                sb.append(" ");
            }
        }
        return sb.toString().trim();
    }

    /**
     * Match a group of one or more whitespace characters including space, tab
     * and newline. This is typically used to split a string into distinct
     * terms.
     */
    public static final String REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS = "\\s+";
    private static final String REGEX_ZERO_OR_MORE_NON_WHITESPACE_CHARS = "[^\\s]*";
    private static final String REGEX_SINGLE_WHITESPACE = "[\\s]";
    
    /**
     * {@code REGEX_PERCENT_SIGN_WITHOUT_ESCAPE_CHAR} Matches the percent sign without escape character[%].
     */
    public static final String REGEX_PERCENT_SIGN_WITHOUT_ESCAPE_CHAR = "(?<!\\\\)%";
    
    /**
     * {@code REGEX_PERCENT_SIGN_WITH_ESCAPE_CHAR} Matches the percent sign without escape character[\%].
     */ 
    public static final String REGEX_PERCENT_SIGN_WITH_ESCAPE_CHAR = "\\\\%";

    private TStrings() {/* utility class */}

}
