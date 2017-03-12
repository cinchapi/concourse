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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.Variables;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Strings;
import com.google.common.collect.Lists;

/**
 * Unit tests for the {@link Strings} utility class.
 * 
 * @author Jeff Nelson
 */
public class StringsTest {

    @Test
    public void testSplitStringByDelimterAndRespectQuotes() {
        String string = "Sachin,,M,\"Maths,Science,English\",Need to improve in these subjects.";
        String[] splitted = Strings.splitStringByDelimiterButRespectQuotes(
                string, ",");
        Assert.assertEquals(
                "[Sachin, , M, \"Maths,Science,English\", Need to improve in these subjects.]",
                Arrays.toString(splitted));
    }

    @Test
    public void testSplitStringByDelimeterWithTrailiningSpaceAndRespectQuotes() {
        String string = "\"New Leaf, Same Page \"";
        String[] splitted = Strings.splitStringByDelimiterButRespectQuotes(
                string, ",");
        Assert.assertEquals("[\"New Leaf, Same Page \"]",
                Arrays.toString(splitted));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSplitWithSingleQuotes() {
        String string = "John said 'hello world'";
        String[] toks = Strings.splitButRespectQuotes(string);
        Assert.assertEquals(
                Lists.newArrayList("John", "said", "\"hello world\""),
                Lists.newArrayList(toks));
    }

    @Test
    public void testTryParseValidNumber() {
        Number valid = Random.getNumber();
        String string = valid.toString();
        if(valid instanceof Double) {
            string += "D";
        }
        Assert.assertEquals(Strings.tryParseNumber(string), valid);
    }

    @Test
    public void testTryParseInvalidNumber() {
        String invalid = Random.getStringNoDigits();
        Assert.assertNull(Strings.tryParseNumber(invalid));
    }

    @Test
    public void testTryParseCoercedDouble() {
        Double d = Variables.register("double", Random.getDouble());
        Assert.assertEquals(d, Strings.tryParseNumber(d + "D"));
    }

    @Test
    public void testSplitCamelCase() {
        String str = "getArg1Arg2Arg3ABC";
        Assert.assertEquals(Lists.newArrayList("get", "Arg1", "Arg2", "Arg3",
                "A", "B", "C"), Strings.splitCamelCase(str));
        str = "testSplitCamelCase";
        Assert.assertEquals(
                Lists.newArrayList("test", "Split", "Camel", "Case"),
                Strings.splitCamelCase(str));
        str = "SplitCamelCase";
        Assert.assertEquals(Lists.newArrayList("Split", "Camel", "Case"),
                Strings.splitCamelCase(str));
        str = "Splitcamelcase";
        Assert.assertEquals(Lists.newArrayList("Splitcamelcase"),
                Strings.splitCamelCase(str));
        str = "splitcamelcase";
        Assert.assertEquals(Lists.newArrayList("splitcamelcase"),
                Strings.splitCamelCase(str));
    }

    @Test
    public void testFormat() {
        String pattern = "This is a string {} that needs to have {} some random {} substitution";
        Object a = Random.getObject();
        Object b = Random.getObject();
        Object c = Random.getObject();
        String expected = "This is a string " + a + " that needs to have " + b
                + " some random " + c + " substitution";
        String actual = Strings.format(pattern, a, b, c);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testIsSubStringReproA() {
        Assert.assertTrue(Strings
                .isSubString(
                        "qrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4we",
                        "b6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qrqrqq40 078rh2n75kxu4prmgtlehv85iksxgehj5jk2prq66ls9bj2f6g5onx l18sgp7x414cik9tvpfycmhjgwhy9d3yhw4web6r4e7g8f8sgu1cjfo16rg711cmft76wh83dsf46wwz3fse5j9chut37nhamqm4iw2f37ebl8tqr4fjmx8n6t943s4khdsf1qr"));
    }

    @Test
    public void testIsSubString() {
        String needle = Variables.register("needle", Random.getString());
        String haystack = Variables.register("haystack", Random.getString());
        Assert.assertEquals(haystack.contains(needle),
                Strings.isSubString(needle, haystack));
    }

    @Test
    public void testIsValidJsonObject() {
        Assert.assertTrue(Strings
                .isValidJson("{\"foo\": 1, \"bar\": \"2\", \"baz\":true}"));
    }

    @Test
    public void testIsValidJsonArray() {
        Assert.assertTrue(Strings
                .isValidJson("[{\"foo\": 1, \"bar\": \"2\", \"baz\":true},{\"foo\": 1, \"bar\": \"2\", \"baz\":true},{\"foo\": 1, \"bar\": \"2\", \"baz\":true}]"));
    }

    @Test
    public void testIsValidJsonFalse() {
        Assert.assertFalse(Strings.isValidJson("foo"));
        Assert.assertFalse(Strings
                .isValidJson("{\"foo\": 1, \"bar\": \"2\", \"baz\":}"));
    }

    @Test
    public void testTryParseFloat() {
        float f = 0.3f;
        Object obj = Strings.tryParseNumber("" + f + "");
        Assert.assertTrue(obj instanceof Float);
        Assert.assertEquals(0.3f, obj);
    }

    @Test
    public void testTryParseDoubleAsFloat() {
        double f = 0.3;
        Object obj = Strings.tryParseNumber("" + f + "");
        Assert.assertTrue(obj instanceof Float);
        Assert.assertEquals(0.3f, obj);
    }

    @Test
    public void testEscapeInnerDoubleQuote() {
        String string = "this has a \"double\" quote and 'single' quote";
        String expected = "this has a \\\"double\\\" quote and 'single' quote";
        Assert.assertEquals(expected, Strings.escapeInner(string, '"'));
    }

    @Test
    public void testEscapeInnerSingleQuote() {
        String string = "this has a 'single' quote and \"double\" quote";
        String expected = "this has a \\'single\\' quote and \"double\" quote";
        Assert.assertEquals(expected, Strings.escapeInner(string, '\''));
    }

    @Test
    public void testEscapeInnerNothing() {
        String string = "this should not be escaped";
        String expected = string;
        Assert.assertEquals(expected, Strings.escapeInner(string, '\"'));
    }

    @Test
    public void testEscapeInnerSingleAndDoubleQuotes() {
        String string = "this has a \"double\" and 'single' quote";
        String expected = "this has a \\\"double\\\" and \\'single\\' quote";
        Assert.assertEquals(expected, Strings.escapeInner(string, '"', '\''));
    }

    @Test
    public void testEscapeInnerNothingSkipHeadTail() {
        String string = "\"this should not be escaped\"";
        String expected = string;
        Assert.assertEquals(expected, Strings.escapeInner(string, '\"'));
    }

    @Test
    public void testEscapeInnerDoubleQuoteSkipHeadTail() {
        String string = "\"this has a \"double\" and 'single' quote\"";
        String expected = "\"this has a \\\"double\\\" and 'single' quote\"";
        Assert.assertEquals(expected, Strings.escapeInner(string, '\"'));
    }

    @Test
    public void testEscapeInnerLineBreak() {
        String string = "\"a\n\nb\"";
        String expected = "\"a\\n\\nb\"";
        Assert.assertEquals(expected, Strings.escapeInner(string, '\n'));
    }

    @Test
    public void testDoNotParseStringAsNumberWithLeadingZero() {
        Assert.assertNull(Strings.tryParseNumber("01"));
    }

    @Test
    public void testParseStringAsNumberIfDecimalWithLeadingZero() {
        Assert.assertTrue(Strings.tryParseNumberStrict("0.0123") instanceof Number);
    }

    @Test
    public void testEnsureStartsWithAlreadyTrue() {
        String prefix = Random.getString();
        String string = prefix + Random.getString();
        Assert.assertTrue(Strings.ensureStartsWith(string, prefix).startsWith(
                prefix));
    }

    @Test
    public void testEnsureStartsWithNotAlreadyTrue() {
        String prefix = Random.getString();
        String string = null;
        while (string == null || string.startsWith(prefix)) {
            string = Random.getString();
        }
        Assert.assertTrue(Strings.ensureStartsWith(string, prefix).startsWith(
                prefix));
    }

    @Test
    public void testEnsureWithinQuotesIfNeeded() {
        String string = "a b c";
        Assert.assertEquals(string,
                Strings.ensureWithinQuotesIfNeeded(string, ','));
        string = "a, b c";
        Assert.assertEquals(Strings.format("\"{}\"", string),
                Strings.ensureWithinQuotesIfNeeded(string, ','));
        string = "a, b \"c";
        Assert.assertEquals(Strings.format("'{}'", string),
                Strings.ensureWithinQuotesIfNeeded(string, ','));
        string = "a, b 'c";
        Assert.assertEquals(Strings.format("\"{}\"", string),
                Strings.ensureWithinQuotesIfNeeded(string, ','));
        string = "a, 'b' \"c\"";
        Assert.assertEquals("\"a, 'b' \\\"c\\\"\"",
                Strings.ensureWithinQuotesIfNeeded(string, ','));
    }

    @Test
    public void testEscapeInnerWhenAlreadyEscaped() {
        String string = "this is a \\\"real\\\" \"real\" problem";
        string = Strings.ensureWithinQuotes(string);
        String expected = "\"this is a \\\"real\\\" \\\"real\\\" problem\"";
        Assert.assertEquals(expected,
                Strings.escapeInner(string, string.charAt(0)));
    }

    @Test
    public void testTryParseNumberIpAddress() {
        Assert.assertNull(Strings.tryParseNumber("23.229.8.250"));
    }

    @Test
    public void testTryParseNumberPeriod() {
        Assert.assertNull(Strings.tryParseNumber("."));
    }

    @Test
    public void testIsWithinQuotesQuotedEmptyString() {
        Assert.assertFalse(Strings.isWithinQuotes("\"\""));
        Assert.assertFalse(Strings.isWithinQuotes("\'\'"));
    }

    @Test
    public void testEnsureWithinQuotesQuotedEmptyString() {
        String string = "\"\"";
        Assert.assertEquals("\"\"\"\"", Strings.ensureWithinQuotes(string));
    }
    
    @Test
    public void testReplaceUnicodeConfusables(){
        String expected = "\"a\"";
        Assert.assertEquals(expected, Strings.replaceUnicodeConfusables(expected));
        Assert.assertEquals(expected, Strings.replaceUnicodeConfusables("˝a˝"));
        Assert.assertEquals(expected, Strings.replaceUnicodeConfusables("″a‶"));
    }

}
