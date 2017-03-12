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
package com.cinchapi.concourse.util;

import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;

import static com.cinchapi.concourse.util.SplitOption.*;

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
     * An integer that contains bits representing {@link SplitOption split
     * options} that have been enabled. To check whether an option is enabled do
     * 
     * <pre>
     * return (options &amp; (1 &lt;&lt; option.mask())) != 0;
     * </pre>
     */
    protected final int options;

    /**
     * The current position of the splitter.
     */
    protected int pos = 0;

    /**
     * The char array of the string that is being split.
     */
    private char[] chars;

    /**
     * The delimiter to use for splitting.
     */
    private final char delimiter;

    /**
     * A flag that controls whether an attempt to split on a newline character
     * sequence should ignore the line feed character ('\n') because the
     * previous character was a carriage return (\r). Typically, a sequence of
     * \r\n is used by Windows to signify a newline.
     * 
     * <p>
     * This flag is only relevant if the option to {@link #splitOnNewline()} is
     * enabled.
     * </p>
     */
    private boolean ignoreLF = false;

    /**
     * A flag that is set in the {@link #next()} method whenever it grabs a
     * {@link #next} token that was determined to be at the end of line. This
     * means that calls to {@link #atEndOfLine()} will return {@code true} until
     * the next call to {@link #next()}.
     */
    private boolean lastEOL = false;

    /**
     * The next string to return.
     */
    private String next = null;

    /**
     * A flag that is set in the {@link #findNext()} method whenever it
     * determines that the {@link #next} token to be returned is at the end of
     * line.
     */
    private boolean nextEOL = false;

    /**
     * A flag that controls whether we should allow {@link #findNext()} to set
     * {@link #next} to an empty string. Normally, whenever two delimiters
     * appear back to back, the splitter will return an empty string (i.e.
     * "foo,,bar,car" means that there is an empty token in the 2nd column).
     * However, when additional {@link #options} are passed to the splitter, it
     * may be unintuitive to return an empty string when we a character that is
     * relevant for one of the options and the delimiter appear back-to-back.
     */
    private boolean overrideEmptyNext = false;

    /**
     * The start of the next token.
     */
    private int start = 0;

    /**
     * Construct a new instance.
     * 
     * @param string the string to split
     */
    public StringSplitter(String string) {
        this(string, ' ');
    }

    /**
     * Construct a new instance.
     * 
     * @param string the string to split
     * @param delimiter the delimiter upon which to split
     */
    public StringSplitter(String string, char delimiter) {
        this(string, delimiter, SplitOption.NONE);
    }

    /**
     * Construct a new instance.
     * 
     * @param string the string to split
     * @param delimiter the delimiter upon which to split
     * @param options an array of {@link SplitOption options} to supplement the
     *            split behaviour
     */
    public StringSplitter(String string, char delimiter, SplitOption... options) {
        this.chars = string.toCharArray();
        this.delimiter = delimiter;
        int opts = 0;
        for (SplitOption option : options) {
            opts |= 1 << option.mask();
        }
        this.options = opts;
        findNext();
    }

    /**
     * Construct a new instance.
     * 
     * @param string the string to split
     * @param options an array of {@link SplitOption options} to supplement the
     *            split behaviour
     */
    public StringSplitter(String string, SplitOption... options) {
        this(string, ' ', options);
    }

    /**
     * Return {@code true} if {@link SplitOption#SPLIT_ON_NEWLINE} is
     * {@link SplitOption#isEnabled(StringSplitter) enabled} and the last token
     * returned by {@link #next()} is followed immediately by a line break.
     * Otherwise, return {@code false}.
     * 
     * @return {@code true} if the last token returned was at the end of line
     */
    public boolean atEndOfLine() {
        return lastEOL;
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
            if(lastEOL) {
                lastEOL = false;
            }
            if(nextEOL) {
                lastEOL = true;
                nextEOL = false;
            }
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
     * Return an array that contains all the tokens after traversing through the
     * entire split process.
     * 
     * @return the tokens
     */
    public String[] toArray() {
        List<String> toks = Lists.newArrayList();
        while (hasNext()) {
            toks.add(next());
        }
        return toks.toArray(new String[0]);
    }

    /**
     * Before an attempt is made to {@link #setNext() set the next token} do
     * some analysis on the internal state of the splitter to see if its
     * actually appropriate to do so. If the next token should not be set,
     * return {@code false} from this method and also optionally change the
     * {@link #pos} pointer to rewind the splitter.
     * 
     * @return {@code true} if the splitter is indeed ready to set the next
     *         token
     */
    protected boolean confirmSetNext() {
        return true;
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
        nextEOL = false;
        next = null;
        boolean resetOverrideEmptyNext = true;
        boolean processOverrideEmptyNext = true;
        while (pos < chars.length && next == null) {
            boolean resetIgnoreLF = true;
            char c = chars[pos];
            ++pos;
            if(c == delimiter && isReadyToSplit()) {
                setNext();
            }
            else if(SPLIT_ON_NEWLINE.isEnabled(this) && c == '\n'
                    && isReadyToSplit()) {
                if(ignoreLF) {
                    start = pos;
                }
                else {
                    setNext();
                    nextEOL = true;
                }
            }
            else if(SPLIT_ON_NEWLINE.isEnabled(this) && c == '\r'
                    && isReadyToSplit()) {
                ignoreLF = true;
                resetIgnoreLF = false;
                setNext();
                nextEOL = true;
            }
            else if(TOKENIZE_PARENTHESIS.isEnabled(this)
                    && (c == '(' || c == ')') && isReadyToSplit()) {
                setNext();
                if(next.isEmpty()) {
                    next = Strings.valueOfCached(c);
                    overrideEmptyNext = true;
                    processOverrideEmptyNext = false;
                    resetOverrideEmptyNext = false;
                }
                else {
                    // Need to undo the modifications from #setNext() in order
                    // to look at the parenthesis char again so it can be
                    // returned as a single token via the if block above
                    pos--;
                    start = pos;
                }
            }
            // For SPLIT_ON_NEWLINE, we must reset #ignoreLF if the current char
            // is not == '\r'
            ignoreLF = resetIgnoreLF ? false : ignoreLF;
            updateIsReadyToSplit(c);
        }
        if(pos == chars.length && next == null) { // If we reach the end of the
                                                  // string without finding
                                                  // the delimiter, then set
                                                  // next to be all the
                                                  // remaining chars.
            if(confirmSetNext()) {
                int length = pos - start;
                if(length == 0) {
                    next = "";
                }
                else {
                    length = trim(length);
                    next = String.valueOf(chars, start, length);
                }
                ++pos;
            }
            else {
                findNext();
            }
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
        // FOR TOKENIZE_PARENTHESIS, we must #overrideEmptyNext if the last
        // next was a single parenthesis in case the next char is a delimiter.
        // This prevents the appearance of having back-to-back delimiters.
        if(overrideEmptyNext && processOverrideEmptyNext) {
            if(next != null && next.isEmpty()) {
                findNext();
            }
            resetOverrideEmptyNext = true;
        }
        overrideEmptyNext = resetOverrideEmptyNext ? false : overrideEmptyNext;
        if(next != null && DROP_QUOTES.isEnabled(this)
                && Strings.isWithinQuotes(next)
                && this instanceof QuoteAwareStringSplitter) {
            next = next.substring(1, next.length() - 1);
        }
    }

    /**
     * Set the {@link #next} element based on the current {@link #pos} and the
     * {@link #start} of the search.
     * <p>
     * The side effects of this method are:
     * <ul>
     * <li>{@code next} is set equal to all the chars from {@link #start} and
     * {@link #pos} - 2</li>
     * <li>{@code start} is set equal to {@link #pos}</li>
     * </li>The char at {@link #pos} - 1 is "dropped". This character is usually
     * the delimiter, so it is okay to do this, but if there is a corner case,
     * the caller must explicitly handle that character
     * </ul>
     * </p>
     */
    private void setNext() {
        if(confirmSetNext()) {
            int length = pos - start - 1;
            if(length == 0) {
                next = "";
            }
            else {
                length = trim(length);
                next = String.valueOf(chars, start, length);
            }
            start = pos;
        }
        else {
            findNext();
        }
    }

    /**
     * Given the desired {@code length} for the {@link #next} token, perform any
     * trimming of leading and trailing white space if
     * {@link SplitOption#TRIM_WHITESPACE}
     * {@link SplitOption#isEnabled(StringSplitter) is enabled}.
     * <p>
     * This method will modify the global {@link #start} position for the
     * {@link #next} string. It returns the appropriate length to assign after
     * the trimming has been done.
     * </p>
     * 
     * @param length the length of the untrimmed {@link #next} string.
     * @return the appropriate length after the trimming
     */
    private int trim(int length) {
        if(SplitOption.TRIM_WHITESPACE.isEnabled(this)) {
            while (Character.isWhitespace(chars[start]) && length > 1) {
                start++;
                length--;
            }
            while (Character.isWhitespace(chars[(start + length) - 1])
                    && length > 1) {
                length--;
            }
        }
        return length;
    }

}