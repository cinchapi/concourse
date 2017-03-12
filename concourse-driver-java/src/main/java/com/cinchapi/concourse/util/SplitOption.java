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

/**
 * An option that can be passed to a {@link StringSplitter} to specify
 * additional split behaviour to supplement the delimiter (i.e. always splitting
 * on a newline).
 * 
 * @author Jeff Nelson
 */
public enum SplitOption {
    /**
     * Split on a newline character sequence (\n, \r\n, \r) in addition to
     * splitting on the delimiter. This is useful for simulating the reading a
     * string, line by line while also splitting on delimiters within that line
     * without traversing the string multiple times.
     */
    SPLIT_ON_NEWLINE(0),

    /**
     * In addition to splitting on the delimiter, split on an any kind of
     * parenthesis and return the same as an individual token.
     */
    TOKENIZE_PARENTHESIS(1),

    /**
     * For the {@link QuoteAwareStringSplitter} drop any quotes that surround a
     * quoted sequence that may contain a delimiter.
     */
    DROP_QUOTES(2),

    /**
     * Trim any leading and trailing whitespace from each token.
     */
    TRIM_WHITESPACE(3);

    /**
     * A constant that signifies no split options should be passed to the
     * {@link StringSplitter}.
     */
    public static SplitOption[] NONE = new SplitOption[] {};

    /**
     * The bit mask to use when flipping/checking the appropriate bit to
     * determine if this option is enabled.
     */
    private final int mask;

    /**
     * Construct a new instance.
     * 
     * @param mask
     */
    private SplitOption(int mask) {
        this.mask = mask;
    }

    /**
     * Return the bit mask for this {@link SplitOption}. Always use this as
     * opposed to {@link #ordinal()}.
     * 
     * @return the bit mask
     */
    public int mask() {
        return mask;
    }

    /**
     * Given a {@link StringSplitter} instance, check to see if this
     * {@link SplitOption option} is enabled. An option can be enabled by
     * passing it to the
     * {@link StringSplitter#StringSplitter(String, char, SplitOption...)
     * constructor}.
     * 
     * @param splitter the {@link StringSplitter} to check
     * @return {@code true} if the bit corresponding to this option is set in
     *         {@code options}
     */
    public boolean isEnabled(StringSplitter splitter) {
        // splitter.options is the value that results from enabling the
        // appropriate bits for all the enabled values.
        return (splitter.options & (1 << mask)) != 0;
    }
}
