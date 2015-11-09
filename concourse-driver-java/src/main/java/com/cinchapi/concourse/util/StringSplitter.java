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

import java.util.NoSuchElementException;

/**
 * An in-place utility to traverse and split a string into substring.
 * <p>
 * Unlike the {@link String#split(String)} method, this utility returns tokens
 * as they are split on the fly so the caller can process them in place. The
 * traditional {@link String#split(String)} approach must make at least two
 * passes over the string [O(n^2)] whereas this approach is guarantee to make a
 * single pass [O(n)].
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
public class StringSplitter {

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
     * Determine, based on state factors that are recorded within the class, if
     * the splitter is actually ready to split the string on an instance of the
     * delimiter. By default, this method always returns {@code true}, but a
     * subclass can use it for awareness of certain conditions that would mean a
     * string should not be split on an instance of the delimiter (i.e. if the
     * delimiter occurs within quotes).
     * 
     * @return {@code true} if the splitter is actually ready to perform a split
     */
    protected boolean isReadyToSplit() {
        return true;
    }

    /**
     * Given a character {@code c} that is processed by the splitter, update the
     * state that determines whether the splitter would actually be ready to
     * split in the event that it encounters a delimiter character.
     * 
     * @param c
     */
    protected void updateIsReadyToSplit(char c) {/* noop */}

    /**
     * Find the next element to return.
     */
    private void findNext() {
        next = null;
        while (pos < chars.length && next == null) {
            char c = chars[pos];
            ++pos;
            if(c == delimiter && isReadyToSplit()) {
                int length = pos - start - 1;
                if(length == 0) {
                    next = "";
                }
                else {
                    next = String.valueOf(chars, start, length);
                }
                start = pos;
            }
            updateIsReadyToSplit(c);

        }
        if(pos == chars.length && next == null) { // If we reach the end of the
                                                  // string without finding
                                                  // the delimiter, then set
                                                  // next to be all the
                                                  // remaining chars.
            int length = pos - start;
            if(length == 0) {
                next = "";
            }
            else {
                next = String.valueOf(chars, start, length);
            }
            ++pos;
        }
        if(next != null && next.isEmpty()) {
            // For compatibility with String#split, we must detect if an empty
            // token occurs at the end of a string by trying to find the next
            // occurrence of a non delimiter char.
            boolean atEnd = true;
            for (int i = pos; i < chars.length; ++i) {
                if(chars[i] != delimiter) {
                    atEnd = false;
                    break;
                }
            }
            next = atEnd ? null : next;
        }
    }

}