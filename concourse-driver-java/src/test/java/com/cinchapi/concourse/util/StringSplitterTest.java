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
import java.util.Queue;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.StringSplitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link StringSplitter} class.
 * 
 * @author Jeff Nelson
 */
public class StringSplitterTest extends ConcourseBaseTest {

    @Test
    public void testStringSplitter() {
        String string = Random.getString();
        char delimiter = string.charAt(Math.abs(Random.getInt()
                % string.length()));
        doTestStringSplitter(string, delimiter);
    }

    @Test
    public void testStringSplitterReproA() {
        doTestStringSplitter("wnwo69", 'w');
    }

    @Test
    public void testStringSplitterReproB() {
        doTestStringSplitter(
                "0n5g6kk2e1wqmwgei4dt b x65 2tglnwrktk8 3xur3rt9i7q z qfbux4ivhpv hpn1om6wmhhvahag5 4xe5rt6oo",
                'o');
    }

    @Test
    public void testStringSplitterReproC() {
        doTestStringSplitter("yj6", 'y');
    }

    @Test
    public void testStringSplitterBackToBackDelims() {
        doTestStringSplitter("w  8", ' ');
    }

    /**
     * Execute the logic for the StringSplitter test.
     * 
     * @param string - The string to split
     * @param delimiter - The delimiter to use when splitting
     */
    private void doTestStringSplitter(String string, char delimiter) {
        Variables.register("string", string);
        Variables.register("delimiter", delimiter);
        StringSplitter splitter = new StringSplitter(string, delimiter);
        List<String> actual = Lists.newArrayList();
        while (splitter.hasNext()) {
            actual.add(splitter.next());
        }
        List<String> expected = Lists.newArrayList(string.split(String
                .valueOf(delimiter)));
        Variables.register("expected", expected);
        Variables.register("actual", actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSplitOnNewlineEnabled() {
        StringSplitter splitter = new StringSplitter("foo", 'o',
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertTrue(SplitOption.SPLIT_ON_NEWLINE.isEnabled(splitter));
        splitter = new StringSplitter("foo", 'o');
        Assert.assertFalse(SplitOption.SPLIT_ON_NEWLINE.isEnabled(splitter));
    }

    @Test
    public void testSplitOnNewlineLF() {
        Set<String> expected = Sets.newHashSet("line1", "line2", "line3");
        String string = Strings.join('\n', expected.toArray());
        StringSplitter it = new StringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        while (it.hasNext()) {
            Assert.assertTrue(expected.contains(it.next()));
        }
    }

    @Test
    public void testSplitOnNewlineCR() {
        Set<String> expected = Sets.newHashSet("line1", "line2", "line3");
        String string = Strings.join('\r', expected.toArray());
        StringSplitter it = new StringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        while (it.hasNext()) {
            Assert.assertTrue(expected.contains(it.next()));
        }
    }

    @Test
    public void testSplitOnNewlineCRLF() {
        Set<String> expected = Sets.newHashSet("line1", "line2", "line3");
        String string = Strings.join("\r\n", expected.toArray());
        StringSplitter it = new StringSplitter(string,
                SplitOption.SPLIT_ON_NEWLINE);
        while (it.hasNext()) {
            Assert.assertTrue(expected.contains(it.next()));
        }
    }

    @Test
    public void testSplitOnCommaAndNewline() {
        String string = "a,b,c\n1,2,3\n4,5,6\n";
        StringSplitter it = new StringSplitter(string, ',',
                SplitOption.SPLIT_ON_NEWLINE);
        Assert.assertEquals("a", it.next());
        Assert.assertEquals("b", it.next());
        Assert.assertEquals("c", it.next());
        Assert.assertEquals("1", it.next());
        Assert.assertEquals("2", it.next());
        Assert.assertEquals("3", it.next());
        Assert.assertEquals("4", it.next());
        Assert.assertEquals("5", it.next());
        Assert.assertEquals("6", it.next());
    }

    @Test
    public void testTokenizeParenthesisAndDelimeterBackToBack() {
        String string = "foo(bar),baz ,,()(bang,";
        StringSplitter it = new StringSplitter(string, ',',
                SplitOption.TOKENIZE_PARENTHESIS);
        Assert.assertEquals("foo", it.next());
        Assert.assertEquals("(", it.next());
        Assert.assertEquals("bar", it.next());
        Assert.assertEquals(")", it.next());
        Assert.assertEquals("baz ", it.next());
        Assert.assertEquals("", it.next());
        Assert.assertEquals("(", it.next());
        Assert.assertEquals(")", it.next());
        Assert.assertEquals("(", it.next());
        Assert.assertEquals("bang", it.next());
    }

    @Test
    public void testTokenizeParenthesis() {
        String string = "foo(bar)";
        StringSplitter it = new StringSplitter(string,
                SplitOption.TOKENIZE_PARENTHESIS);
        while (it.hasNext()) {
            Assert.assertEquals("foo", it.next());
            Assert.assertEquals("(", it.next());
            Assert.assertEquals("bar", it.next());
            Assert.assertEquals(")", it.next());
        }
    }

    @Test
    public void testEndOfLineLF() {
        String string = "a b c\n\nd e f";
        StringSplitter it = new StringSplitter(string, ' ',
                SplitOption.SPLIT_ON_NEWLINE);
        Queue<String> queue = Queues.newSingleThreadedQueue();
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        queue.offer("");
        queue.offer("d");
        queue.offer("e");
        queue.offer("f");
        while (it.hasNext()) {
            String expected = queue.poll();
            Assert.assertEquals(expected, it.next());
            if(expected.equals("c") || expected.equals("")) {
                Assert.assertTrue(it.atEndOfLine());
                Assert.assertTrue(it.atEndOfLine());
            }
            else {
                Assert.assertFalse(it.atEndOfLine());
                Assert.assertFalse(it.atEndOfLine());
            }
        }
    }

    @Test
    public void testEndOfLineCR() {
        String string = "a b c\r\rd e f";
        StringSplitter it = new StringSplitter(string, ' ',
                SplitOption.SPLIT_ON_NEWLINE);
        Queue<String> queue = Queues.newSingleThreadedQueue();
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        queue.offer("");
        queue.offer("d");
        queue.offer("e");
        queue.offer("f");
        while (it.hasNext()) {
            String expected = queue.poll();
            Assert.assertEquals(expected, it.next());
            if(expected.equals("c") || expected.equals("")) {
                Assert.assertTrue(it.atEndOfLine());
                Assert.assertTrue(it.atEndOfLine());
            }
            else {
                Assert.assertFalse(it.atEndOfLine());
                Assert.assertFalse(it.atEndOfLine());
            }
        }
    }

    @Test
    public void testEndOfLineCRLF() {
        String string = "a b c\r\n\r\nd e f";
        StringSplitter it = new StringSplitter(string, ' ',
                SplitOption.SPLIT_ON_NEWLINE);
        Queue<String> queue = Queues.newSingleThreadedQueue();
        queue.offer("a");
        queue.offer("b");
        queue.offer("c");
        queue.offer("");
        queue.offer("d");
        queue.offer("e");
        queue.offer("f");
        while (it.hasNext()) {
            String expected = queue.poll();
            Assert.assertEquals(expected, it.next());
            if(expected.equals("c") || expected.equals("")) {
                Assert.assertTrue(it.atEndOfLine());
                Assert.assertTrue(it.atEndOfLine());
            }
            else {
                Assert.assertFalse(it.atEndOfLine());
                Assert.assertFalse(it.atEndOfLine());
            }
        }
    }
    
    @Test
    public void testTrimTokens(){
        String string = "a, b, c, d, e";
        StringSplitter it = new StringSplitter(string, ',', SplitOption.TRIM_WHITESPACE);
        while(it.hasNext()){
            Assert.assertFalse(it.next().contains(" "));
        }
    }
    
    @Test
    public void testTrimSingleTokenLeading(){
        String string = "  a";
        StringSplitter it = new StringSplitter(string, ',', SplitOption.TRIM_WHITESPACE);
        while(it.hasNext()){
            Assert.assertFalse(it.next().contains(" "));
        }
    }
    
    @Test
    public void testTrimSingleTokenTrailing(){
        String string = "a  ";
        StringSplitter it = new StringSplitter(string, ',', SplitOption.TRIM_WHITESPACE);
        while(it.hasNext()){
            Assert.assertFalse(it.next().contains(" "));
        }
    }
    
    @Test
    public void testTrimTokensLeadingAndTrailing(){
        String string = "  a  ,c  ,  d,e";
        StringSplitter it = new StringSplitter(string, ',', SplitOption.TRIM_WHITESPACE);
        while(it.hasNext()){
            Assert.assertFalse(it.next().contains(" "));
        }
    }
    
}
