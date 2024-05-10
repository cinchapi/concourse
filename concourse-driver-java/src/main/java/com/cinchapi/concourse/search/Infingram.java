/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.search;

import org.apache.commons.lang3.StringUtils;

import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.StringSplitter;

/**
 * An {@link Infingram} is a sequence of tokens (e.g., words), within another
 * {@link String}, {@code t}.
 * <p>
 * Each token from the {@link Infingram} either appear as is or as a substring
 * of a corresponding token in {@code t}, with all tokens maintaining their
 * relative order.
 * </p>
 * <p>
 * {@link Infingram Infingrams} are used for efficient multi-word fuzzy search
 * across values that have not been previously indexed. A search query that
 * is used to construct and {@link Infingram} is compiled for repeated
 * comparisons against potential matches using the {@link #in(String)} method.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class Infingram {

    /**
     * Empty tokens.
     */
    private static final char[][] EMPTY = new char[0][0];

    /**
     * Represents a sequence of tokens where each token is defined as a
     * {@code char[]}. This array structure is used to match a series of tokens
     * within another string in the order they appear in this array. Each token
     * can match as an infix, meaning it can appear anywhere within the target
     * string but must maintain the relative order specified in this array.
     */
    private final char[][] tokens;

    /**
     * Construct a new instance.
     * 
     * @param needle
     */
    public Infingram(String needle) {
        this.tokens = tokenize(needle);
    }

    /**
     * Return {@code true} if this {@link Infingram} (the <em>needle</em>)
     * exists within {@code value} (the <em>haystack</em>) such that it is an
     * <strong>infix search match</strong>. If <em>haystack</em> is an infix
     * search match, it means that it contains a sequence of terms where each
     * term or a substring of the term matches the term in the same relative
     * position of the <em>needle</em>.
     * <p>
     * <ul>
     * <li><em>foo bar</em> (needle) <strong>IS</strong> an infingram in <em>foo
     * bar</em> (haystack)</li>
     * <li><em>f bar</em> (needle) <strong>IS</strong> an infingram in <em>foo
     * bar</em> (haystack)</li>
     * <li><em>oo a</em> (needle) <strong>IS</strong> an infingram in <em>foo
     * bar</em> (haystack)</li>
     * <li><em>f b</em> (needle) <strong>IS</strong> an infingram in <em>foo
     * bar</em> (haystack)</li>
     * <li><em>f bar</em> (needle) <strong>IS</strong> an infingram in
     * <em>barfoobar foobarfoo</em> (haystack)</li>
     * </ul>
     * </p>
     * 
     * @param value the "haystack" value in which to search for this
     *            {@link Infingram infingram}
     * @return {@code true} if this {@link Infingram infingram} is contained
     *         within the {@code value}
     */
    public boolean in(String value) {
        char[][] needle = tokens;
        char[][] haystack = tokenize(value);
        if(needle.length == 0 || haystack.length == 0) {
            // By convention, empty strings are never considered search matches.
            return false;
        }
        else {
            int npos = 0;
            int hpos = 0;
            while (hpos < haystack.length && npos < needle.length) {
                if(haystack.length - hpos < needle.length - npos) {
                    // If the number of remaining haystack tokens is less than
                    // the number of remaining needle tokens, then we can exit
                    // immediately because it is not possible for the needle to
                    // be fond in the haystack
                    return false;
                }
                char[] n = needle[npos];
                char[] h = haystack[hpos];
                if(isSubstring(n, h)) {
                    ++npos;
                    ++hpos;
                }
                else {
                    if(npos > 0) {
                        // If the needle position is greater than 0, then we
                        // must keep the haystack position constant so that we
                        // can use it as the new starting point to see if the
                        // needle can be found in the remaining tokens.
                        npos = 0;
                    }
                    else {
                        ++hpos;
                    }
                }
            }
            return npos == needle.length;
        }

    }

    /**
     * Return the number of tokens.
     * 
     * @return the number of tokens
     */
    public int numTokens() {
        return tokens.length;
    }

    /**
     * Return {@code true} if {@code needle} is a substring of {@code haystack}.
     * <p>
     * This method is optimized compared to
     * {@link String#contains(CharSequence)}.
     * </p>
     * 
     * @param needle
     * @param haystack
     * @return {@code true} if {@code needle} is a substring
     */
    private boolean isSubstring(char[] needle, char[] haystack) {
        // TODO: switch out with Boyer Moore??
        if(needle.length > haystack.length) {
            return false;
        }
        else {
            int npos = 0;
            int hpos = 0;
            int stop = haystack.length - needle.length;
            int hstart = -1;
            while (hpos < haystack.length && npos < needle.length) {
                char hi = haystack[hpos];
                char ni = needle[npos];
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
            return npos == needle.length;
        }
    }

    /**
     * Split {@code value} into tokens and remove {@link GlobalState#STOPWORDS
     * stopwords}.
     * 
     * @param value
     * @return an array of character arrays, each representing a token.
     */
    private char[][] tokenize(String value) {
        ArrayBuilder<char[]> tokens = ArrayBuilder.builder();
        StringSplitter it = new StringSplitter(value.toLowerCase(), ' ');
        while (it.hasNext()) {
            String token = it.next();
            if(!StringUtils.isBlank(token)) {
                tokens.add(token.toCharArray());
            }
        }
        return tokens.length() > 0 ? tokens.build() : EMPTY;
    }

}
