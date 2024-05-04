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
package com.cinchapi.concourse.util;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.StringSplitter;
import com.cinchapi.concourse.server.GlobalState;

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
     * Tokenize the {@code string} and return an array of tokens where all the
     * stopwords are removed.
     * 
     * @param string
     * @return the tokens without stopwords
     */
    public static String[] stripStopWordsAndTokenize(String string) {
        ArrayBuilder<String> toks = ArrayBuilder.builder();
        StringSplitter it = new StringSplitter(string, ' ');
        while (it.hasNext()) {
            String next = it.next();
            if(!StringUtils.isBlank(next)
                    && !GlobalState.STOPWORDS.contains(next)) {
                toks.add(next);
            }
        }
        return toks.length() > 0 ? toks.build() : Array.containing();
    }
    
    // public static char[] stripStopWordsAndGatherChars(String string)

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
