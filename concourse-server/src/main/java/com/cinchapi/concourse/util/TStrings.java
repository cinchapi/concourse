/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import com.cinchapi.common.base.AnyStrings;

/**
 * String based utility functions that depend on proprietary information that is
 * specific to Concourse (i.e. the stopwords defined for a Concourse Server
 * deployment).
 * 
 * The {@link Strings} class contains a collection of truly generic String
 * functions.
 * 
 * @author Jeff Nelson
 */
public final class TStrings {

    /**
     * <p>
     * Unlike {@link #isInfixSearchMatch(String, String)} this method assumes
     * that the two parameters are tokens of strings that have had stop words
     * removed using the {@link #stripStopWordsAndTokenize(String)} method.
     * </p>
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
    public static boolean isInfixSearchMatch(String[] needle,
            String[] haystack) {
        int npos = 0;
        int hpos = 0;
        while (hpos < haystack.length && npos < needle.length) {
            if(haystack.length - hpos < needle.length - npos) {
                // If the number of remaining haystack tokens is less than the
                // number of remaining needle tokens, then we can exit
                // immediately because it is not possible for the needle to be
                // fond in the haystack
                return false;
            }
            String n = needle[npos];
            String h = haystack[hpos];
            if(AnyStrings.isSubString(n, h)) {
                ++npos;
                ++hpos;
            }
            else {
                // If the needle position is greater than 0, then we must keep
                // the haystack position constant so that we can use it as the
                // new starting point to see if the needle can be found in the
                // remaining tokens.
                if(npos > 0) {
                    npos = 0;
                }
                else {
                    ++hpos;
                }
            }
        }
        return npos == needle.length;
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
        String[] ntoks = needle
                .split(REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
        String[] htoks = haystack
                .split(REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
        return isInfixSearchMatch(ntoks, htoks);
    }

    /**
     * Match a group of one or more whitespace characters including space, tab
     * and newline. This is typically used to split a string into distinct
     * terms.
     */
    public static final String REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS = "\\s+";
    /**
     * {@code REGEX_PERCENT_SIGN_WITH_ESCAPE_CHAR} Matches the percent sign
     * without escape character[\%].
     */
    public static final String REGEX_PERCENT_SIGN_WITH_ESCAPE_CHAR = "\\\\%";
    /**
     * {@code REGEX_PERCENT_SIGN_WITHOUT_ESCAPE_CHAR} Matches the percent sign
     * without escape character[%].
     */
    public static final String REGEX_PERCENT_SIGN_WITHOUT_ESCAPE_CHAR = "(?<!\\\\)%";
    protected static final String REGEX_SINGLE_WHITESPACE = "[\\s]";
    protected static final String REGEX_ZERO_OR_MORE_NON_WHITESPACE_CHARS = "[^\\s]*";

    private TStrings() {/* utility class */}

}
