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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.QuoteAwareStringSplitter;
import com.cinchapi.concourse.util.Strings;

/**
 * Unit tests for the {@link QuoteAwareStringSplitter class}.
 * 
 * @author Jeff Nelson
 */
public class QuoteAwareStringSplitterTest {

    /**
     * Test logic for ensuring that the quote aware string splitter works.
     * 
     * @param string
     * @param delim
     */
    private void doTestSplitWithQuotes(String string, char delim) {
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(string,
                delim);
        String[] toks = Strings.splitStringByDelimiterButRespectQuotes(string,
                String.valueOf(delim));
        int i = 0;
        while (it.hasNext()) {
            String tok = toks[i];
            if(!tok.isEmpty()) {
                String next = it.next();
                if(next.startsWith("'") && next.endsWith("'")) {
                    // Strings#splitStringByDelimiterButRespectQuotes replaces
                    // single quotes with double quotes, so we must do that here
                    // in order to do the comparison
                    next = "\"" + next.substring(1, next.length() - 1) + "\"";
                }
                Assert.assertEquals(tok, next);
            }
            ++i;
        }
    }

    @Test
    public void testSplitWithSingleQuotes() {
        doTestSplitWithQuotes(
                "this string is going to be split by 'space but we are respecting' single quotes",
                ' ');
    }

    @Test
    public void testSplitWithDoubleQuotes() {
        doTestSplitWithQuotes(
                "this string is going to be split by \"space but we are respecting\" double quotes",
                ' ');
    }

    @Test
    public void testDontSplitOnApostrophe() {
        doTestSplitWithQuotes(
                "don't split the string on the apostrophe 'because it needs to work'",
                ' ');
    }

    @Test
    public void testSplitOnNewlineLF() {
        String string = "this\nis a 'quote across\nlines'";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("this", it.next());
        Assert.assertEquals("is", it.next());
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("'quote across\nlines'", it.next());
    }

    @Test
    public void testSplitOnNewlineCR() {
        String string = "this\nis a 'quote across\rlines'";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("this", it.next());
        Assert.assertEquals("is", it.next());
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("'quote across\rlines'", it.next());
    }

    @Test
    public void testSplitOnNewlineCRLF() {
        String string = "this\nis a 'quote across\r\nlines'";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("this", it.next());
        Assert.assertEquals("is", it.next());
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("'quote across\r\nlines'", it.next());
    }

    @Test
    public void testTokenizeParenthesis() {
        String string = "foo(bar) \"but don't (split this)\" but split ( this)";
        StringSplitter it = new QuoteAwareStringSplitter(string,
                SplitOption.TOKENIZE_PARENTHESIS);
        while (it.hasNext()) {
            Assert.assertEquals("foo", it.next());
            Assert.assertEquals("(", it.next());
            Assert.assertEquals("bar", it.next());
            Assert.assertEquals(")", it.next());
            Assert.assertEquals("\"but don't (split this)\"", it.next());
            Assert.assertEquals("but", it.next());
            Assert.assertEquals("split", it.next());
            Assert.assertEquals("(", it.next());
            Assert.assertEquals("this", it.next());
            Assert.assertEquals(")", it.next());
        }
    }

}
