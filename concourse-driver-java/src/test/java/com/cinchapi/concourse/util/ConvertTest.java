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

import static com.cinchapi.concourse.util.Convert.RAW_RESOLVABLE_LINK_SYMBOL_APPEND;
import static com.cinchapi.concourse.util.Convert.RAW_RESOLVABLE_LINK_SYMBOL_PREPEND;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Strings;
import com.cinchapi.concourse.util.Convert.ResolvableLink;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

    @Test(expected = JsonParseException.class)
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
    public void testConvertJsonStringNumberReproA() {
        Number value = 0.5907014118838035;
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
        String value = MessageFormat.format("{0}{1}", "@", number.toString()); // must
                                                                               // use
                                                                               // number.toString()
                                                                               // so
                                                                               // comma
                                                                               // separators
                                                                               // are
                                                                               // not
                                                                               // added
                                                                               // to
                                                                               // the
                                                                               // output
        Link link = (Link) Convert.stringToJava(value);
        Assert.assertEquals(number.intValue(), link.intValue());
    }

    @Test
    public void testConvertLinkFromLongValue() {
        // A int/long that is wrapped between two at (@) symbols must always
        // convert to a Link
        Number number = Random.getLong();
        String value = MessageFormat.format("{0}{1}", "@", number.toString()); // must
                                                                               // use
                                                                               // number.toString()
                                                                               // so
                                                                               // comma
                                                                               // separators
                                                                               // are
                                                                               // not
                                                                               // added
                                                                               // to
                                                                               // the
                                                                               // output
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
        String ccl = Strings.joinWithSpace(key, "=", value);
        ResolvableLink link = (ResolvableLink) Convert.stringToJava(Convert
                .stringToResolvableLinkInstruction(ccl));
        Assert.assertEquals(ccl, link.getCcl());
    }

    @Test
    public void testConvertResolvableLinkWithNumbers() {
        String key = Random.getNumber().toString();
        String value = Random.getNumber().toString();
        String ccl = Strings.joinWithSpace(key, "=", value);
        ResolvableLink link = (ResolvableLink) Convert.stringToJava(Convert
                .stringToResolvableLinkInstruction(ccl));
        Assert.assertEquals(ccl, link.getCcl());
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

    @SuppressWarnings("deprecation")
    @Test
    public void testTransformValueToResolvableLink() {
        String key = Random.getString();
        String value = Random.getObject().toString();
        String expected = Strings.joinSimple("@",
                Strings.joinWithSpace(key, "=", value), "@");
        Assert.assertEquals(expected,
                Convert.stringToResolvableLinkSpecification(key, value));
    }

    @Test
    public void testStringToOperator() {
        String symbol = "=";
        Assert.assertTrue(Convert.stringToOperator(symbol) instanceof Operator);
    }

    @Test
    public void testDoubleEqualsStringToOperatorEquals() {
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
    public void testStringLnks2ToOperatorLinksTo() {
        String symbol = "lnks2";
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

    @Test
    public void testConvertMapToJson() {
        Map<String, Collection<Object>> map = Maps.newHashMap();
        Set<Object> aValues = Sets.newHashSet();
        Set<Object> bValues = Sets.newHashSet();
        Set<Object> cValues = Sets.newHashSet();
        aValues.add(1);
        aValues.add("1");
        aValues.add(1.00);
        bValues.add(true);
        cValues.add("hello");
        cValues.add("hello world");
        map.put("a", aValues);
        map.put("b", bValues);
        map.put("c", cValues);
        String converted = Convert.mapToJson(map);
        Assert.assertEquals(map, Convert.jsonToJava(converted).asMap());
    }

    @Test
    public void testConvertMapWithEmptyCollectionToJson() {
        // This test is here to document the current practice of output an
        // emptying JSON array when converting empty collections. If, in the
        // future, we decide to output null, this test should be updated to
        // reflect that.
        Map<String, Collection<Object>> map = Maps.newHashMap();
        Set<Object> aValues = Sets.newHashSet();
        Set<Object> bValues = Sets.newHashSet();
        Set<Object> cValues = Sets.newHashSet();
        aValues.add(1);
        aValues.add("1");
        aValues.add(1.00);
        bValues.add(true);
        map.put("a", aValues);
        map.put("b", bValues);
        map.put("c", cValues);
        String expected = "{\"b\":true,\"c\":[],\"a\":[1,\"1\",\"1.0D\"]}";
        Assert.assertTrue(expected.contains("\"c\":[]"));

    }

    @Test
    public void testConvertMultimapToJson() {
        Multimap<String, Object> map = LinkedHashMultimap.create();
        map.put("a", 1);
        map.put("a", "1");
        map.put("a", 1.00);
        map.put("b", true);
        map.put("c", "hello");
        map.put("c", "hello world");
        String expected = "{\"a\":[1,\"1\",\"1.0D\"],\"b\":true,\"c\":[\"hello\",\"hello world\"]}";
        Assert.assertEquals(expected, Convert.mapToJson(map));
    }

    @Test
    public void testConvertStringLinkToJava() {
        long record = Random.getLong();
        String strLink = "@" + record;
        Link link = (Link) Convert.stringToJava(strLink);
        Assert.assertEquals(record, link.longValue());
    }

    @Test
    public void testConvertJsonWithMasqueradingDouble() {
        String json = "{\"double\": \"3.14D\"}";
        Multimap<String, Object> converted = Convert.jsonToJava(json);
        Assert.assertEquals(3.14, converted.get("double").iterator().next());
    }

    @Test
    public void testConvertMapsToJson() {
        List<Multimap<String, Object>> list = Lists.newArrayList();
        Multimap<String, Object> a = LinkedHashMultimap.create();
        Multimap<String, Object> b = LinkedHashMultimap.create();
        Multimap<String, Object> c = LinkedHashMultimap.create();
        a.put("a", 1);
        a.put("b", 2);
        a.put("c", 3);
        b.put("a", "a");
        b.put("a", 1);
        b.put("b", "b");
        b.put("b", 2);
        b.put("c", 3);
        b.put("c", "c");
        c.put("a", true);
        c.put("a", false);
        c.put("a", 1.1);
        c.put("b", 3.14);
        c.put("b", "hello");
        c.put("b", "world");
        c.put("c", "me");
        c.put("c", 0);
        c.put("c", 4L);
        list.add(a);
        list.add(b);
        list.add(c);
        Assert.assertEquals(
                "[{\"a\":1,\"b\":2,\"c\":3},{\"a\":[\"a\",1],\"b\":[\"b\",2],\"c\":[3,\"c\"]},{\"a\":[true,false,\"1.1D\"],\"b\":[\"3.14D\",\"hello\",\"world\"],\"c\":[\"me\",0,4]}]",
                Convert.mapsToJson(list));
    }

    @Test
    public void testMultimapWithLinkObjectToJson() {
        Multimap<String, Object> data = HashMultimap.create();
        data.put("foo", Link.to(1));
        String json = Convert.mapToJson(data);
        Assert.assertEquals("{\"foo\":\"@1\"}", json);
    }

    @Test
    public void testMapWithLinkObjectToJson() { // GH-120
        Map<String, Object> data = Maps.newHashMap();
        data.put("foo", Link.to(1));
        String json = Convert.mapToJson(data);
        Assert.assertEquals("{\"foo\":\"@1\"}", json);
    }

    @Test
    public void testConvertEmptyString() {
        Assert.assertEquals("", Convert.stringToJava(""));
    }

    @Test
    public void testConvertJsonArrayReproA() {
        String json = "[{\"id\":34,\"handle\":\".tp-caption.medium_bg_orange\",\"settings\":\"{\\\"hover\\\":\\\"false\\\"}\",\"hover\":\"\",\"params\":'{\"color\":\"rgb(255, 255, 255)\",\"font-size\":\"20px\",\"line-height\":\"20px\",\"font-weight\":\"800\",\"font-family\":\"\\\"Open Sans\\\"\",\"text-decoration\":\"none\",\"padding\":\"10px\",\"background-color\":\"rgb(243, 156, 18)\",\"border-width\":\"0px\",\"border-color\":\"rgb(255, 214, 88)\",\"border-style\":\"none\"}',\"__table\":\"wp_revslider_css\"}]";
        Convert.anyJsonToJava(json);
        Assert.assertTrue(true); // lack of Exception means the test passes
    }

    @Test
    public void testPossibleThriftToJavaAlreadyJava() {
        Object expected = Random.getObject();
        Object actual = Convert.possibleThriftToJava(expected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testPossibleThriftToJavaScalarTObject() {
        Object expected = Random.getObject();
        TObject texpected = Convert.javaToThrift(expected);
        Object actual = Convert.possibleThriftToJava(texpected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testPossibleThriftToJavaListMixed() {
        List<Object> expected = Lists.newArrayList();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; ++i) {
            expected.add(Random.getObject());
        }
        List<Object> texpected = Lists.newArrayList();
        for (Object object : expected) {
            if(Random.getInt() % 2 == 0) {
                texpected.add(Convert.javaToThrift(object));
            }
            else {
                texpected.add(object);
            }
        }
        List<Object> actual = Convert.possibleThriftToJava(texpected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testPossibleThriftToJavaSetMixed() {
        Set<Object> expected = Sets.newHashSet();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; ++i) {
            expected.add(Random.getObject());
        }
        Set<Object> texpected = Sets.newHashSet();
        for (Object object : expected) {
            if(Random.getInt() % 2 == 0) {
                texpected.add(Convert.javaToThrift(object));
            }
            else {
                texpected.add(object);
            }
        }
        Set<Object> actual = Convert.possibleThriftToJava(texpected);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testPossibleThriftToJavaMapMixed() {
        Map<Object, Object> expected = Maps.newHashMap();
        int count = Random.getScaleCount();
        for (int i = 0; i < count; ++i) {
            Object key = Random.getObject();
            Object value = Random.getObject();
            expected.put(key, value);
        }
        Map<Object, Object> texpected = Maps.newHashMap();
        for (Entry<Object, Object> object : expected.entrySet()) {
            Object key = object.getKey();
            Object value = object.getValue();
            int seed = Random.getInt();
            if(seed % 5 == 0) {
                key = Convert.javaToThrift(key);
            }
            else if(seed % 4 == 0) {
                value = Convert.javaToThrift(value);
            }
            else if(seed % 3 == 0) {
                key = Convert.javaToThrift(key);
                value = Convert.javaToThrift(value);
            }
            texpected.put(key, value);
        }
        Map<Object, Object> actual = Convert.possibleThriftToJava(texpected);
        Assert.assertEquals(expected, actual);
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
