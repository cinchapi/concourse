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
        if(c == '\'' && !inDoubleQuote) {
            inSingleQuote ^= true;
        }
        else if(c == '"' && !inSingleQuote) {
            inDoubleQuote ^= true;
        }
    }

}
