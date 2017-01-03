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
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.concourse.importer.Transformer;
import com.cinchapi.concourse.util.KeyValue;
import com.cinchapi.concourse.util.QuoteAwareStringSplitter;
import com.cinchapi.concourse.util.SplitOption;
import com.cinchapi.concourse.util.StringBuilderWriter;
import com.cinchapi.concourse.util.StringSplitter;
import com.cinchapi.concourse.util.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.stream.JsonWriter;

/**
 * Helper functions to aid in the process of importing data from files or
 * strings.
 * 
 * @author Jeff Nelson
 */
public class Importables {

    /**
     * Given a string of {@code lines}, where is line has tokens that are
     * separated by a {@code delimiter} character, covert each line to a JSON
     * object and return a single string that contains a JSON array of the
     * objects.
     * 
     * @param lines the data to convert
     * @param resolveKey a key that the
     *            {@link com.cinchapi.concourse.importer.Importer importer} uses
     *            to determine the existing records into which the data should
     *            be imported. The method places the appropriate resolve
     *            instruction in the JSON blob so Concourse Server can perform
     *            the resolution during the import
     * @param delimiter the delimiter character
     * @param header the list of header keys. If this list is empty, it is
     *            assumed that the first line of {@code lines} contains the
     *            header
     * @param transformer the {@link Transformer} that is used to potentially
     *            alter key/value pairs before import
     * @return a JSON string that contains the raw {@code lines} in the new
     *         format
     * @throws IndexOutOfBoundsException if a line has more columns that there
     *             are header keys; it is fine for a line to have fewer columns
     *             that header keys
     */
    public static String delimitedStringToJsonArray(String lines,
            @Nullable String resolveKey, char delimiter, List<String> header,
            @Nullable Transformer transformer) {
        header = header == null ? Lists.<String> newArrayList() : header;
        StringSplitter it = createStringSplitter(lines, delimiter);
        StringBuilderWriter out = new StringBuilderWriter();
        int hcount = 0;
        try (JsonWriter json = new JsonWriter(out)) {
            json.beginArray();
            while (it.hasNext()) {
                if(header.isEmpty()) {
                    do {
                        header.add(it.next());
                    }
                    while (it.hasNext() && !it.atEndOfLine());
                }
                else {
                    json.beginObject();
                    hcount = 0;
                    do {
                        String key = header.get(hcount);
                        ++hcount;
                        String value = it.next();
                        Object jvalue = value;
                        KeyValue<String, Object> kv = transformer == null ? null
                                : transformer.transform(key, value);
                        if(kv != null) {
                            key = kv.getKey();
                            jvalue = kv.getValue();
                            value = jvalue.toString();
                        }
                        // TODO process resolve key
                        json.name(key);
                        if(StringUtils.isBlank(value)) {
                            json.nullValue();
                        }
                        else if(jvalue instanceof Collection) {
                            json.beginArray();
                            for (Object jitem : (Collection<?>) jvalue) {
                                writeJsonValue(json, jitem.toString());
                            }
                            json.endArray();
                        }
                        else {
                            writeJsonValue(json, value);
                        }
                    }
                    while (it.hasNext() && !it.atEndOfLine());
                    json.endObject();
                }
            }
            json.endArray();
            return out.toString();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Given a {@code line} that is separated by a {@code delimiter} character,
     * convert each token to fields within a single JSON object.
     * 
     * @param line the data to convert
     * @param resolveKey a key that the
     *            {@link com.cinchapi.concourse.importer.Importer importer} uses
     *            to determine the existing records into which the data should
     *            be imported. The method places the appropriate resolve
     *            instruction in the JSON blob so Concourse Server can perform
     *            the resolution during the import
     * @param delimiter the delimiter character
     * @param header the list of header keys. If this list is empty, this method
     *            will return {@code null} and instead place the tokens from the
     *            {@code line} into the list
     * @param transformer the {@link Transformer} that is used to potentially
     *            alter key/value pairs before import
     * @return a JSON string that contains the raw {@code line} in the new
     *         format or {@code null} if {@code header} is
     *         {@link List#isEmpty() empty}
     * @throws IndexOutOfBoundsException if the line has more columns that there
     *             are header keys (as long as the number of header keys is >
     *             0); it is fine for a line to have fewer columns that header
     *             keys
     */
    @Nullable
    public static String delimitedStringToJsonObject(String line,
            @Nullable String resolveKey, char delimiter, List<String> header,
            @Nullable Transformer transformer) {
        StringBuilder builder = new StringBuilder();
        delimitedStringToJsonObject(line, resolveKey, delimiter, header,
                transformer, builder);
        return builder.length() == 0 ? null : builder.toString();
    }

    /**
     * Given a {@code line} that is separated by a {@code delimiter} character,
     * convert each token to fields within a single JSON object.
     * <p>
     * This method does not return anything. It writes the JSON blob to the
     * provided {@code builder}.
     * </p>
     * 
     * @param line the data to convert
     * @param resolveKey a key that the
     *            {@link com.cinchapi.concourse.importer.Importer importer} uses
     *            to determine the existing records into which the data should
     *            be imported. The method places the appropriate resolve
     *            instruction in the JSON blob so Concourse Server can perform
     *            the resolution during the import
     * @param delimiter the delimiter character
     * @param header the list of header keys. If this list is empty, this method
     *            will return {@code null} and instead place the tokens from the
     *            {@code line} into the list
     * @param transformer the {@link Transformer} that is used to potentially
     *            alter key/value pairs before import
     * @param builder the {@link StringBuilder} where the JSON blob should be
     *            written
     * @throws IndexOutOfBoundsException if the line has more columns that there
     *             are header keys (as long as the number of header keys is >
     *             0); it is fine for a line to have fewer columns that header
     *             keys
     */
    public static void delimitedStringToJsonObject(String line,
            @Nullable String resolveKey, char delimiter, List<String> header,
            @Nullable Transformer transformer, StringBuilder builder) {
        StringSplitter it = createStringSplitter(line, delimiter);
        if(header.isEmpty()) {
            while (it.hasNext()) {
                header.add(it.next());
            }
        }
        else {
            int hcount = 0;
            StringBuilderWriter out = new StringBuilderWriter(builder);
            try (JsonWriter json = new JsonWriter(out)) {
                json.beginObject();
                while (it.hasNext()) {
                    String key = header.get(hcount);
                    ++hcount;
                    String value = it.next();
                    Object jvalue = value;
                    KeyValue<String, Object> kv = transformer == null ? null
                            : transformer.transform(key, value);
                    if(kv != null) {
                        key = kv.getKey();
                        jvalue = kv.getValue();
                        value = jvalue.toString();
                    }
                    // TODO process resolve key
                    json.name(key);
                    if(StringUtils.isBlank(value)) {
                        json.nullValue();
                    }
                    else if(jvalue instanceof Collection) {
                        json.beginArray();
                        for (Object jitem : (Collection<?>) jvalue) {
                            writeJsonValue(json, jitem.toString());
                        }
                        json.endArray();
                    }
                    else {
                        writeJsonValue(json, value);
                    }
                }
                json.endObject();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Intelligently write the appropriate JSON representation for {@code value}
     * to {@code out}.
     * 
     * @param out the {@link JsonWriter} to use for writing
     * @param value the value to write
     * @throws IOException
     */
    @VisibleForTesting
    protected static void writeJsonValue(JsonWriter out, String value)
            throws IOException {
        Object parsed;
        if((parsed = Strings.tryParseNumberStrict(value)) != null) {
            out.value((Number) parsed);
        }
        else if((parsed = Strings.tryParseBoolean(value)) != null) {
            out.value((boolean) parsed);
        }
        else {
            value = Strings.ensureWithinQuotes(value);
            value = Strings.escapeInner(value, value.charAt(0), '\n');
            out.jsonValue(value);
        }
    }

    /**
     * Return a {@link StringSplitter} for {@code string} and {@code delimiter}
     * with all the appropriate {@link SplitOption options}.
     * 
     * @param string the string over which to split
     * @param delimiter the delimiter on which to split
     * @return an appropriately configured {@link StringSplitter}
     */
    private static StringSplitter createStringSplitter(String string,
            char delimiter) {
        return new QuoteAwareStringSplitter(string, delimiter,
                SplitOption.TRIM_WHITESPACE, SplitOption.SPLIT_ON_NEWLINE);
    }

}
