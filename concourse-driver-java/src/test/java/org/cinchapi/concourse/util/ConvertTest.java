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

import static org.cinchapi.concourse.util.Convert.RAW_RESOLVABLE_LINK_SYMBOL_APPEND;
import static org.cinchapi.concourse.util.Convert.RAW_RESOLVABLE_LINK_SYMBOL_PREPEND;

import java.text.MessageFormat;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert.ResolvableLink;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.JsonParseException;

/**
 * Unit tests for the {@link Convert} utility class
 * 
 * @author Jeff Nelson
 */
public class ConvertTest {

    @Test(expected = JsonParseException.class)
    public void testCannConvertJsonArrayWithNonPrimitives() {
        Convert.jsonToJava("{\"key\": [1, [2, 3]]}");
    }

    @Test(expected = JsonParseException.class)
    public void testCannontConvertInvalidJsonString() {
        Convert.jsonToJava(Random.getString());
    }

    @Test(expected = JsonParseException.class)
    public void testCannotConvertJsonLeadingWithArray() {
        Convert.jsonToJava("[\"a\",\"b\",\"c\"]");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCannotConvertJsonStringWithEmbeddedObject() {
        Convert.jsonToJava("{\"key\": {\"a\": 1}}");
    }

    @Test
    public void testCannotConvertLinkFromBooleanValue() {
        Boolean number = Random.getBoolean();
        String value = MessageFormat
                .format("{0}{1}{0}", "@", number.toString());
        Assert.assertFalse(Convert.stringToJava(value) instanceof Link);
    }

    @Test
    public void testCannotConvertLinkFromDoubleValue() {
        Number number = Random.getDouble();
        String value = MessageFormat
                .format("{0}{1}{0}", "@", number.toString());
        Assert.assertFalse(Convert.stringToJava(value) instanceof Link);
    }

    @Test
    public void testCannotConvertLinkFromFloatValue() {
        Number number = Random.getFloat();
        String value = MessageFormat
                .format("{0}{1}{0}", "@", number.toString());
        Assert.assertFalse(Convert.stringToJava(value) instanceof Link);
    }

    @Test
    public void testCannotConvertLinkFromStringValue() {
        String number = Random.getString();
        while (StringUtils.isNumeric(number)) {
            number = Random.getString();
        }
        String value = MessageFormat
                .format("{0}{1}{0}", "@", number.toString());
        Assert.assertFalse(Convert.stringToJava(value) instanceof Link);
    }

    @Test
    public void testConvertBoolean() {
        Boolean bool = Random.getBoolean();
        String boolString = scrambleCase(bool.toString());
        Assert.assertEquals(bool, Convert.stringToJava(boolString));
    }

    @Test
    public void testConvertDouble() {
        Number number = Random.getDouble();
        String string = number.toString() + "D";
        Assert.assertEquals(number, Convert.stringToJava(string));
    }

    @Test
    public void testConvertFloat() {
        Number number = Random.getFloat();
        String string = number.toString();
        Assert.assertEquals(number, Convert.stringToJava(string));
    }

    @Test
    public void testConvertForcedStringDoubleQuotes() {
        // A value that is wrapped in single (') or double (") quotes must
        // always be converted to a string
        Object object = Random.getObject();
        String value = MessageFormat.format("{0}{1}{0}", "\"",
                object.toString());
        Assert.assertEquals(Convert.stringToJava(value), object.toString());
    }

    @Test
    public void testConvertForcedStringSingleQuotes() {
        // A value that is wrapped in single (') or double (") quotes must
        // always be converted to a string
        Object object = Random.getObject();
        String value = MessageFormat
                .format("{0}{1}{0}", "'", object.toString());
        Assert.assertEquals(Convert.stringToJava(value), object.toString());
    }

    @Test
    public void testConvertInteger() {
        Number number = Random.getInt();
        String string = number.toString();
        Assert.assertEquals(number, Convert.stringToJava(string));
    }

    @Test
    public void testConvertJsonArrayWithTopLevelObject() {
        String json = "{\"foo\": 1, \"bar\": true, \"car\": [\"a\", 1, 2]}";
        List<Multimap<String, Object>> data = Convert.anyJsonToJava(json);
        Assert.assertEquals(1, data.size());
        Assert.assertEquals(data.get(0), Convert.jsonToJava(json));
    }

    @Test
    public void testConvertJsonArrayWithManyObjects() {
        String spec = "{\"foo\": 1, \"bar\": true, \"car\": [\"a\", 1, 2]}";
        int count = Random.getScaleCount();
        String json = "[";
        for (int i = 0; i < count; i++) {
            json += spec + ",";
        }
        json += "]";
        json = json.replace(",]", "]");
        List<Multimap<String, Object>> data = Convert.anyJsonToJava(json);
        Assert.assertEquals(count, data.size());
    }

    @Test
    public void testConvertJsonArray() {
        int intValue = Random.getInt();
        String string = Random.getString();
        boolean bool = Random.getBoolean();
        long longVal = Random.getLong();
        float floatVal = Random.getFloat();
        double doubleVal = Random.getDouble();
        Set<Object> expected = Sets.<Object> newHashSet(intValue, string, bool,
                longVal, floatVal, doubleVal);
        String json = "{\"array\": [" + intValue + ", \"" + string + "\", "
                + bool + ", " + longVal + ", " + floatVal + ", \"" + doubleVal
                + "D\"]}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Set<Object> values = Sets.newHashSet(data.get("array"));
        Set<Object> oddMenOut = Sets.symmetricDifference(expected, values);
        Assert.assertEquals(0, oddMenOut.size());
    }

    @Test
    public void testConvertJsonArrayDupesAreFilteredOut() {
        String json = "{\"key\": [3, 3]}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(1, data.size());
    }

    @Test
    public void testConvertJsonBoolean() {
        boolean value = Random.getBoolean();
        String json = "{\"elt\": " + value + "}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(value, Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonDouble() {
        double value = Random.getDouble();
        String json = "{\"elt\": \"" + value + "D\"}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(value, Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonFloat() {
        float value = Random.getFloat();
        String json = "{\"elt\": " + value + "}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(value, Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonInteger() {
        int value = Random.getInt();
        String json = "{\"elt\": " + value + "}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(value, Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonLong() {
        long value = Random.getLong();
        String json = "{\"elt\": " + value + "}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(value, Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonString() {
        String value = Random.getString();
        String json = "{\"elt\": \"" + value + "\"}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals(value, Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonStringBoolean() {
        boolean value = Random.getBoolean();
        String json = "{\"elt\": \"" + value + "\"}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals("" + value + "",
                Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertJsonStringNumber() {
        Number value = Random.getNumber();
        String json = "{\"elt\": \"" + value + "\"}";
        Multimap<String, Object> data = Convert.jsonToJava(json);
        Assert.assertEquals("" + value + "",
                Iterables.getOnlyElement(data.get("elt")));
    }

    @Test
    public void testConvertLinkFromIntValue() {
        // A int/long that is wrapped between two at (@) symbols must always
        // convert to a Link
        Number number = Random.getInt();
        String value = MessageFormat
                .format("{0}{1}{0}", "@", number.toString()); // must use
                                                              // number.toString()
                                                              // so comma
                                                              // separators are
                                                              // not added to
                                                              // the output
        Link link = (Link) Convert.stringToJava(value);
        Assert.assertEquals(number.intValue(), link.intValue());
    }

    @Test
    public void testConvertLinkFromLongValue() {
        // A int/long that is wrapped between two at (@) symbols must always
        // convert to a Link
        Number number = Random.getLong();
        String value = MessageFormat
                .format("{0}{1}{0}", "@", number.toString()); // must use
                                                              // number.toString()
                                                              // so comma
                                                              // separators are
                                                              // not added to
                                                              // the output
        Link link = (Link) Convert.stringToJava(value);
        Assert.assertEquals(number.longValue(), link.longValue());
    }

    @Test
    public void testConvertLong() {
        Number number = null;
        while (number == null || (Long) number <= Integer.MAX_VALUE) {
            number = Random.getLong();
        }
        String string = number.toString();
        Assert.assertEquals(number, Convert.stringToJava(string));
    }

    @Test
    public void testConvertResolvableLink() {
        String key = Random.getString().replace(" ", "");
        String value = Random.getObject().toString().replace(" ", "");
        ResolvableLink link = (ResolvableLink) Convert.stringToJava(Convert
                .stringToResolvableLinkSpecification(key, value));
        Assert.assertEquals(link.key, key);
        Assert.assertEquals(link.value, Convert.stringToJava(value));
    }

    @Test
    public void testConvertResolvableLinkWithNumbers() {
        String key = Random.getNumber().toString();
        String value = Random.getNumber().toString();
        ResolvableLink link = (ResolvableLink) Convert.stringToJava(Convert
                .stringToResolvableLinkSpecification(key, value));
        Assert.assertEquals(link.key, key);
        Assert.assertEquals(link.value, Convert.stringToJava(value));
    }

    @Test
    public void testConvertTag() {
        String string = Random.getString();
        while (string.contains("`")) {
            string = Random.getString();
        }
        String value = MessageFormat.format("{0}{1}{0}", "`", string);
        Assert.assertTrue(Convert.stringToJava(value) instanceof Tag);
    }

    @Test
    public void testResolvableLinkKeyAndValueRegexWithNumbers() {
        String key = RAW_RESOLVABLE_LINK_SYMBOL_PREPEND
                + Random.getNumber().toString()
                + RAW_RESOLVABLE_LINK_SYMBOL_APPEND;
        String string = key + Random.getNumber().toString() + key;
        Assert.assertTrue(string.matches(MessageFormat.format("{0}{1}{0}",
                MessageFormat.format("{0}{1}{2}",
                        RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, ".+",
                        RAW_RESOLVABLE_LINK_SYMBOL_APPEND), ".+")));
    }

    @Test
    public void testResolvableLinkKeyRegexWithNumbers() {
        String string = RAW_RESOLVABLE_LINK_SYMBOL_PREPEND
                + Random.getNumber().toString()
                + RAW_RESOLVABLE_LINK_SYMBOL_APPEND;
        Assert.assertTrue(string.matches(MessageFormat.format("{0}{1}{2}",
                RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, ".+",
                RAW_RESOLVABLE_LINK_SYMBOL_APPEND)));
    }

    @Test
    public void testTransformValueToResolvableLink() {
        String key = Random.getString();
        String value = Random.getObject().toString();
        Assert.assertEquals(MessageFormat.format("{0}{1}{0}", MessageFormat
                .format("{0}{1}{2}", RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, key,
                        RAW_RESOLVABLE_LINK_SYMBOL_APPEND), value), Convert
                .stringToResolvableLinkSpecification(key, value));
    }

    @Test
    public void testStringToOperator() {
        String symbol = "=";
        Assert.assertTrue(Convert.stringToOperator(symbol) instanceof Operator);
    }
    
    @Test
    public void testDoubleEqualsStringToOperatorEquals(){
        String string = "==";
        Assert.assertEquals(Convert.stringToOperator(string), Operator.EQUALS);
    }

    @Test
    public void testStringToOperatorEquals() {
        String string = "=";
        Assert.assertEquals(Convert.stringToOperator(string), Operator.EQUALS);
    }

    @Test
    public void testSymbolToOperatorEquals() {
        String symbol = "eq";
        Assert.assertEquals(Convert.stringToOperator(symbol), Operator.EQUALS);
    }

    @Test
    public void testStringToOperatorNotEquals() {
        String symbol = "!=";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.NOT_EQUALS);
    }

    @Test
    public void testSymbolToOperatorNotEquals() {
        String symbol = "ne";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.NOT_EQUALS);
    }

    @Test
    public void testStringToOperatorGreaterThan() {
        String symbol = ">";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.GREATER_THAN);
    }

    @Test
    public void testSymbolToOperatorGreaterThan() {
        String symbol = "gt";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.GREATER_THAN);
    }

    @Test
    public void testStringToOperatorGreaterThanOrEquals() {
        String symbol = ">=";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.GREATER_THAN_OR_EQUALS);
    }

    @Test
    public void testSymbolToOperatorGreaterThanOrEquals() {
        String symbol = "gte";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.GREATER_THAN_OR_EQUALS);
    }

    @Test
    public void testStringToOperatorLessThan() {
        String symbol = "<";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.LESS_THAN);
    }

    @Test
    public void testSymbolToOperatorLessThan() {
        String symbol = "lt";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.LESS_THAN);
    }

    @Test
    public void testStringToOperatorLessThanOrEquals() {
        String symbol = "<=";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.LESS_THAN_OR_EQUALS);
    }

    @Test
    public void testSymbolToOperatorLessThanOrEquals() {
        String symbol = "lte";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.LESS_THAN_OR_EQUALS);
    }

    @Test
    public void testStringToOperatorBetween() {
        String symbol = "><";
        Assert.assertEquals(Convert.stringToOperator(symbol), Operator.BETWEEN);
    }

    @Test
    public void testSymbolToOperatorBetween() {
        String symbol = "bw";
        Assert.assertEquals(Convert.stringToOperator(symbol), Operator.BETWEEN);
    }

    @Test
    public void testStringToOperatorLinksTo() {
        String symbol = "->";
        Assert.assertEquals(Convert.stringToOperator(symbol), Operator.LINKS_TO);
    }

    @Test
    public void testSymbolToOperatorLinksTo() {
        String symbol = "lnk2";
        Assert.assertEquals(Convert.stringToOperator(symbol), Operator.LINKS_TO);
    }

    @Test
    public void testSymbolToOperatorRegex() {
        String symbol = "regex";
        Assert.assertEquals(Convert.stringToOperator(symbol), Operator.REGEX);
    }

    @Test
    public void testSymbolToOperatorNotRegex() {
        String symbol = "nregex";
        Assert.assertEquals(Convert.stringToOperator(symbol),
                Operator.NOT_REGEX);
    }

    /**
     * Randomly flip the case of all the characters in {@code string}.
     * 
     * @param string
     * @return the case scrambled string
     */
    private String scrambleCase(String string) {
        char[] chars = string.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if(Random.getInt() % 2 == 0) {
                c = Character.toLowerCase(c);
            }
            else {
                c = Character.toUpperCase(c);
            }
            chars[i] = c;
        }
        return new String(chars);
    }

}
