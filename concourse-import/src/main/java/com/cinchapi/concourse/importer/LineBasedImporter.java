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
package com.cinchapi.concourse.importer;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.QuoteAwareStringSplitter;
import com.cinchapi.concourse.util.Strings;
import com.cinchapi.concourse.util.TLists;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * An {@link Importer} that handles data from a file that can be delimited into
 * one or more lines. Each line is considered a single group of data that can be
 * converted to a multimap and imported in one or more records in Concourse.
 * 
 * @author Jeff Nelson
 */
@Deprecated
public abstract class LineBasedImporter extends JsonImporter {

    /**
     * A flag that indicates whether the importer should use the optimized split
     * path that takes advantage of the {@link QuoteAwareStringSplitter}.
     */
    protected boolean useOptimizedSplitPath = true; // visible for testing

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected LineBasedImporter(Concourse concourse, Logger log) {
        super(concourse, log);
        useOptimizedSplitPath = delimiter().length() == 1;
    }

    @Override
    public final Set<Long> importFile(String file) {
        return importFile(file, null);
    }

    /**
     * Import the data contained in {@code file} into {@link Concourse}.
     * <p>
     * <strong>Note</strong> that if {@code resolveKey} is specified, an attempt
     * will be made to add the data in from each group into the existing records
     * that are found using {@code resolveKey} and its corresponding value in
     * the group.
     * </p>
     * 
     * @param file
     * @param resolveKey
     * @return a collection of {@link ImportResult} objects that describes the
     *         records created/affected from the import and whether any errors
     *         occurred.
     */
    public final Set<Long> importFile(String file, @Nullable String resolveKey) {
        // TODO add option to specify batchSize, which is how many objects to
        // send over the wire in one atomic batch
        List<String> lines = FileOps.readLines(file);
        String[] keys = header();
        JsonArray array = new JsonArray();
        boolean checkedFileFormat = false;
        for (String line : lines) {
            if(!checkedFileFormat) {
                validateFileFormat(line);
                checkedFileFormat = true;
            }
            if(keys == null) {
                keys = parseKeys(line);
                 log.info("Parsed keys from header: " + line);
            }
            else {
                JsonObject object = parseLine(line, keys);
                if(resolveKey != null && object.has(resolveKey)) {
                    JsonElement resolveValue = object.get(resolveKey);
                    if(!resolveValue.isJsonArray()) {
                        JsonArray temp = new JsonArray();
                        temp.add(resolveValue);
                        resolveValue = temp;
                    }
                    for (int i = 0; i < resolveValue.getAsJsonArray().size(); ++i) {
                        String value = resolveValue.getAsJsonArray().get(i)
                                .toString();
                        Object stored = Convert.stringToJava(value);
                        Set<Long> resolved = concourse.find(resolveKey,
                                Operator.EQUALS, stored);
                        for (long record : resolved) {
                            object = parseLine(line, keys); // this is
                                                            // inefficient, but
                                                            // there is no good
                                                            // way to clone the
                                                            // original object
                            object.addProperty(
                                    Constants.JSON_RESERVED_IDENTIFIER_NAME,
                                    record);
                            array.add(object);
                        }
                    }
                }
                else {
                    array.add(object);
                }
                 log.info("Importing {}", line);
            }

        }
        Set<Long> records = importString(array.toString());
        return records;
    }

    /**
     * The delimiter that is used to split fields on each line.
     * 
     * @return the delimiter
     */
    protected abstract String delimiter();

    /**
     * This method is provided so the subclass can provide an ordered array of
     * headers if they are not provided in the file as the first line.
     * 
     * @return the header information
     */
    protected String[] header() {
        return null;
    }

    /**
     * At a minimum, this method is responsible for taking a raw source string
     * value and converting it to a {@link JsonElement}. The default
     * implementation makes an effort to represent numeric and boolean values as
     * appropriate {@link JsonPrimitive json primitives}. All other kinds of
     * values are represented as strings, which is the correct format for the
     * server to handle masquerading types (i.e. resolvable links, links, tags,
     * forced doubles, etc).
     * <p>
     * The default behaviour is appropriate in most cases, but this method can
     * be used by subclasses to define dynamic intermediary transformations to
     * data to better prepare it for import.
     * </p>
     * <h1>Examples</h1> <h2>Specifying Link Resolution</h2>
     * <p>
     * The server will convert raw data of the form
     * <code>@&lt;key&gt;@value@&lt;key&gt;@</code> into a Link to all the
     * records where key equals value in Concourse. For this purpose, the
     * subclass can convert the raw value to this form using the
     * {@link Convert#stringToResolvableLinkSpecification(String, String)}
     * method.
     * </p>
     * <p>
     * <h2>Normalizing Data</h2>
     * It may be desirable to normalize the raw data before input. For example,
     * the subclass may wish to convert all strings to a specific case, or
     * sanitize inputs, etc.
     * </p>
     * <p>
     * <h2>Compacting Representation</h2>
     * If a column in a file contains a enumerated set of string values, it may
     * be desirable to transform the values to a string representation of a
     * number so that, when converted, the data is more compact and takes up
     * less space.
     * </p>
     * 
     * @param key
     * @param value
     * @return the transformed values in a JsonArray
     */
    protected JsonElement transformValue(String key, String value) {
        JsonPrimitive element;
        Object parsed;
        if((parsed = Strings.tryParseNumberStrict(value)) != null) {
            element = new JsonPrimitive((Number) parsed);
        }
        else if((parsed = Strings.tryParseBoolean(value)) != null) {
            element = new JsonPrimitive((Boolean) parsed);
        }
        else {
            element = new JsonPrimitive(value);
            element.toString();
        }
        return element;
    }

    /**
     * Check {@code line} to determine if is valid for the the file format that
     * is supported by the importer.
     * 
     * @param line is a line of the file being imported
     * @throws IllegalArgumentException if the line from the file is
     *             not acceptable for the file format
     * 
     */
    protected abstract void validateFileFormat(String line)
            throws IllegalArgumentException;

    /**
     * Parse the keys from the {@code line}. The delimiter can be specified by
     * the subclass in the {@link #delimiter()} method.
     * 
     * @param line
     * @return an array of keys
     */
    private final String[] parseKeys(String line) {
        String[] keys = null;
        if(useOptimizedSplitPath) {
            QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(line,
                    delimiter().charAt(0));
            List<String> keysList = Lists.newArrayList();
            while (it.hasNext()) {
                keysList.add(it.next().trim());
            }
            keys = TLists.toArrayCasted(keysList, String.class);
        }
        else {
            keys = Strings.splitStringByDelimiterButRespectQuotes(line,
                    delimiter());
            for (int i = 0; i < keys.length; ++i) {
                keys[i] = keys[i].trim();
            }
        }
        return keys;
    }

    /**
     * Parse the data from {@code line} into a {@link JsonObject} that is
     * appropriate for import. The subclass can customize the behaviour of this
     * process by overriding the {@link #header()} and
     * {@link #transformValue(String, String)} methods.
     * 
     * @param line
     * @param keys
     * @return the line data encoded as a JsonObject
     */
    private final JsonObject parseLine(String line, String... keys) {
        line = line.trim();
        JsonObject json = new JsonObject();
        String[] toks = null;
        if(useOptimizedSplitPath) {
            QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(line,
                    delimiter().charAt(0));
            List<String> toksList = Lists.newArrayList();
            while (it.hasNext()) {
                toksList.add(it.next());
            }
            toks = TLists.toArrayCasted(toksList, String.class);
        }
        else {
            toks = Strings.splitStringByDelimiterButRespectQuotes(line,
                    delimiter());
        }
        for (int i = 0; i < Math.min(keys.length, toks.length); ++i) {
            if(StringUtils.isBlank(toks[i])) {
                continue;
            }
            JsonElement value = transformValue(keys[i], toks[i]);
            json.add(keys[i], value);
        }
        return json;
    }
}
