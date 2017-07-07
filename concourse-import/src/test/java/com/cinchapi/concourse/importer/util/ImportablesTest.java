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
package com.cinchapi.concourse.importer.util;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.KeyValue;
import com.cinchapi.concourse.util.StringBuilderWriter;
import com.cinchapi.concourse.util.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.gson.stream.JsonWriter;

/**
 * Unit tests for the {@link Importables} class.
 * 
 * @author Jeff Nelson
 */
public class ImportablesTest {

    @Test
    public void testDelimitedStringToJsonArray() {
        String string = "a,b,c,d,e\n1,\"this has,a comma\",@3@,\"4\",true";
        String json = Importables.delimitedStringToJsonArray(string, null, ',',
                Lists.<String> newArrayList(), null);
        Assert.assertTrue(json.startsWith("["));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testDelimitedStringToJsonObject() {
        List<String> header = Lists.newArrayList("a", "b", "c", "d", "e");
        String string = "1,\"this has,a comma\",@3@,'4',true";
        String json = Importables.delimitedStringToJsonObject(string, null, ',',
                header, null);
        Assert.assertTrue(json.startsWith("{"));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testDelimitedStringToJsonObjectNoHeader() {
        List<String> header = Lists.newArrayList();
        String string = "1,\"this has,a comma\",@3@,'4',true";
        String json = Importables.delimitedStringToJsonObject(string, null, ',',
                header, null);
        Assert.assertNull(json);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testDelimitedStringToJsonArrayTooFewHeaderColumns() {
        String string = "a,b,c,d\n1,\"this has,a comma\",@3@,\"4\",true";
        String json = Importables.delimitedStringToJsonArray(string, null, ',',
                Lists.<String> newArrayList(), null);
        Assert.assertTrue(json.startsWith("["));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testDelimitedStringToJsonObjectTooFewHeaderColumns() {
        List<String> header = Lists.newArrayList("a", "b", "c", "d");
        String string = "1,\"this has,a comma\",@3@,'4',true";
        String json = Importables.delimitedStringToJsonObject(string, null, ',',
                header, null);
        Assert.assertTrue(json.startsWith("{"));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testDelimitedStringToJsonArrayTooManyHeaderColumns() {
        String string = "a,b,c,d\n1,\"this has,a comma\",@3@,";
        String json = Importables.delimitedStringToJsonArray(string, null, ',',
                Lists.<String> newArrayList(), null);
        Assert.assertTrue(json.startsWith("["));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testDelimitedStringToJsonObjectTooManyHeaderColumns() {
        List<String> header = Lists.newArrayList("a", "b", "c", "d");
        String string = "1,\"this has,a comma\",@3@,'4'";
        String json = Importables.delimitedStringToJsonObject(string, null, ',',
                header, null);
        Assert.assertTrue(json.startsWith("{"));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testDelimitedStringToJsonArrayNullValue() {
        String string = "a,b,c,d,e\n1,,@3@,\"4\",true";
        String json = Importables.delimitedStringToJsonArray(string, null, ',',
                Lists.<String> newArrayList(), null);
        Assert.assertTrue(json.startsWith("["));
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testDelimitedStringToJsonObjectEmptyHeaderGetsFilled() {
        String string = "a,b,c,d,e";
        List<String> header = Lists.newArrayList();
        Importables.delimitedStringToJsonObject(string, null, ',', header,
                null);
        Assert.assertEquals(5, header.size());
    }

    @Test
    public void testDelimitedStringToJsonArrayEmptyHeaderGetsFilled() {
        String string = "a,b,c,d,e\n1,,@3@,\"4\",true";
        List<String> header = Lists.newArrayList();
        Importables.delimitedStringToJsonArray(string, null, ',', header, null);
        Assert.assertEquals(5, header.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDelimitedStringToJsonArrayMultiValues() {
        String lines = "a,b\nfoo,bar";
        String resolveKey = null;
        char delimiter = ',';
        List<String> header = Lists.newArrayList();
        String json = Importables.delimitedStringToJsonArray(lines, resolveKey,
                delimiter, header, (key, value) -> {
                    if(key.equals("a")) {
                        return new KeyValue<String, Object>("A",
                                Lists.newArrayList(value.toLowerCase(),
                                        value.toUpperCase(), value.length()));
                    }
                    else {
                        return null;
                    }
                });
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDelimitedStringToJsonObjectMultiValues() {
        String lines = "foo,bar";
        String resolveKey = null;
        char delimiter = ',';
        List<String> header = Lists.newArrayList("a", "b");
        String json = Importables.delimitedStringToJsonObject(lines, resolveKey,
                delimiter, header, (key, value) -> {
                    if(key.equals("a")) {
                        return new KeyValue<String, Object>("A",
                                Lists.newArrayList(value.toLowerCase(),
                                        value.toUpperCase(), value.length()));
                    }
                    else {
                        return null;
                    }
                });
        Assert.assertTrue(Strings.isValidJson(json));
    }

    @Test
    public void testWriteOutJsonNumber() throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonWriter out = new JsonWriter(new StringBuilderWriter(sb));
        out.beginArray();
        Importables.writeJsonValue(out, "1");
        out.endArray();
        Assert.assertEquals("[1]", sb.toString());
    }

    @Test
    public void testWriteOutJsonForcedDouble() throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonWriter out = new JsonWriter(new StringBuilderWriter(sb));
        out.beginArray();
        Importables.writeJsonValue(out, "1D");
        out.endArray();
        Assert.assertEquals("[\"1D\"]", sb.toString());
    }

    @Test
    public void testWriteOutJsonForcedStringSingleQuotes() throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonWriter out = new JsonWriter(new StringBuilderWriter(sb));
        out.beginArray();
        Importables.writeJsonValue(out, "'1'");
        out.endArray();
        Assert.assertEquals("['1']", sb.toString());
    }

    @Test
    public void testWriteOutJsonForcedStringDoubleQuotes() throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonWriter out = new JsonWriter(new StringBuilderWriter(sb));
        out.setLenient(true);
        Importables.writeJsonValue(out, "\"1\"");
        Assert.assertEquals("\"1\"", sb.toString());
    }

    @Test
    public void testWriteOutJsonBoolean() throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonWriter out = new JsonWriter(new StringBuilderWriter(sb));
        out.beginArray();
        Importables.writeJsonValue(out, "true");
        out.endArray();
        Assert.assertEquals("[true]", sb.toString());
    }

    @Test
    public void testWriteOutJsonStringWithLineBreak() throws IOException {
        StringBuilder sb = new StringBuilder();
        JsonWriter out = new JsonWriter(new StringBuilderWriter(sb));
        out.beginArray();
        Importables.writeJsonValue(out, "a\n\nb");
        out.endArray();
        Assert.assertEquals("[\"a\\n\\nb\"]", sb.toString());
    }

    @Test
    public void testTrimWhitespace() {
        String str = "a, b, c, d, e\nz, y, x, w, v";
        String json = Importables.delimitedStringToJsonArray(str, null, ',',
                null, null);
        List<Multimap<String, Object>> data = Convert.anyJsonToJava(json);
        for (Multimap<String, Object> map : data) {
            for (Entry<String, Object> entry : map.entries()) {
                Assert.assertEquals(entry.getKey(), entry.getKey().trim());
                if(entry.getValue() instanceof String) {
                    String value = (String) entry.getValue();
                    Assert.assertEquals(value, value.trim());
                }

            }
        }

    }

}
