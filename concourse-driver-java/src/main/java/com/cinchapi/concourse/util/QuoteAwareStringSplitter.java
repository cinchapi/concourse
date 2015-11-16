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

/**
 * A {@link StringSplitter} that does not split on a delimiter if it falls
 * between single or double quotes
 * 
 * @author Jeff Nelson
 */
public class QuoteAwareStringSplitter extends StringSplitter {

    /**
     * A flag that indicates whether the splitter is in the middle of a single
     * quoted token.
     */
    private boolean inSingleQuote = false;

    /**
     * A flag that indicates whether the splitter is in the middle of a double
     * quoted token.
     */
    private boolean inDoubleQuote = false;

    /**
     * We keep track of the previously seen char so we can determine in the
     * instance of a single quote is really an apostrophe or the beginning of a
     * quoted token.
     */
    private char previousChar = ' ';

    /**
     * Construct a new instance.
     * 
     * @param string
     */
    public QuoteAwareStringSplitter(String string) {
        super(string);
    }

    /**
     * Construct a new instance.
     * 
     * @param string
     * @param delimiter
     */
    public QuoteAwareStringSplitter(String string, char delimiter) {
        super(string, delimiter);
    }

    @Override
    protected boolean isReadyToSplit() {
        assert (inSingleQuote && inDoubleQuote) == false; // assert that we are
                                                          // never both in the
                                                          // context of having
                                                          // both single and
                                                          // double quotes
        return !inSingleQuote && !inDoubleQuote;
    }

    @Override
    protected void updateIsReadyToSplit(char c) {
        if(c == '\''
                && !inDoubleQuote
                && (inSingleQuote || (!inSingleQuote && !Character
                        .isLetter(previousChar)))) {
            // Assumes that occurrence of single quote only means single quote
            // if the previous char was not a letter (in which case we assume
            // the single quote is actually an apostrophe)
            inSingleQuote ^= true;
        }
        else if(c == '"' && !inSingleQuote) {
            inDoubleQuote ^= true;
        }
        previousChar = c;
    }

}
