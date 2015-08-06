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
package org.cinchapi.concourse.util;

import java.util.NoSuchElementException;

/**
 * A utility to traverse and split a string into {@link Substring substrings}
 * in-place.
 * <p>
 * Unlike the {@link String#split(String)} method, this utility returns tokens
 * as they are split on the fly so the caller can process then in place. The
 * traditional {@link String#split(String)} method must make at least two passes
 * over the string [O(n^2)] whereas this approach is guarantee to make a single
 * pass [O(n)].
 * </p>
 * <p>
 * <h2>Usage</h2>
 * 
 * <pre>
 * String string = &quot;Please split this string by space&quot;;
 * StringSplitter splitter = new StringSplitter(string);
 * while (splitter.hasNext()) {
 *     String next = splitter.next();
 * }
 * </pre>
 * 
 * </p>
 * 
 * @author Jeff Nelson
 */
public final class StringSplitter {

    /**
     * The char array of the string that is being split.
     */
    private char[] chars;

    /**
     * The delimiter to use for splitting.
     */
    private final char delimiter;

    /**
     * The next string to return.
     */
    private String next = null;

    /**
     * The current position of the splitter.
     */
    private int pos = 0;

    /**
     * The start of the next token.
     */
    private int start = 0;

    /**
     * Construct a new instance.
     * 
     * @param string
     */
    public StringSplitter(String string) {
        this(string, ' ');
    }

    /**
     * Construct a new instance.
     * 
     * @param string
     * @param delimiter
     */
    public StringSplitter(String string, char delimiter) {
        this.chars = string.toCharArray();
        this.delimiter = delimiter;
        findNext();
    }

    /**
     * Return {@code true} if this splitter has any remaining substrings.
     * 
     * @return {@code true} if there is another element
     */
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Return the next substring that results from splitting the original source
     * string.
     * 
     * @return the new substring
     */
    public String next() {
        if(next == null) {
            throw new NoSuchElementException();
        }
        else {
            String result = next;
            findNext();
            return result;
        }
    }

    /**
     * Reset the splitter.
     */
    public void reset() {
        pos = 0;
        start = 0;
    }

    /**
     * Find the next element to return.
     */
    private void findNext() {
        next = null;
        while (pos < chars.length && next == null) {
            char c = chars[pos];
            ++pos;
            if(c == delimiter) {
                next = String.valueOf(chars, start, (pos - start - 1));
                start = pos;
            }
        }
        if(pos == chars.length && next == null) { // If we reach the end of the
                                                  // string without finding
                                                  // the delimiter, then set
                                                  // next to be all the
                                                  // remaining chars.
            next = String.valueOf(chars, start, pos - start);
            ++pos;
        }
        if(next != null && next.isEmpty()) { // Don't allow next to be an empty
                                             // String, under any circumstances
            findNext();
        }
    }

}