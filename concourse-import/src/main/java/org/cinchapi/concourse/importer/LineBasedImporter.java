/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.importer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.Constants;
import org.cinchapi.concourse.importer.util.Files;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Arrays;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Platform;
import org.cinchapi.concourse.util.Strings;

import ch.qos.logback.classic.Logger;

import com.google.common.base.Throwables;
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
public abstract class LineBasedImporter extends JsonImporter {

    /**
     * The ideal size for the batch of records we include in a single JSON
     * insert/upsert.
     */
    private static int IDEAL_BATCH_SIZE = 20000;

    private final ExecutorService executor = Executors
            .newFixedThreadPool(Platform.idealNumberOfThreadsForCpuBoundTask());

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected LineBasedImporter(Concourse concourse) {
        super(concourse);
    }

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected LineBasedImporter(Concourse concourse, Logger log) {
        super(concourse, log);
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
    @SuppressWarnings("unchecked")
    public final Set<Long> importFile(String file, @Nullable String resolveKey) {
        // TODO add option to specify batchSize, which is how many objects to
        // send over the wire in one atomic batch
        List<String> lines = Files.readLines(file);
        String[] keys = header();
        Object array = resolveKey == null ? new JsonObject[IDEAL_BATCH_SIZE]
                : Lists.newArrayListWithCapacity(IDEAL_BATCH_SIZE);
        AtomicBoolean upsert = new AtomicBoolean(false);
        int slot = 0;
        for (String line : lines) {
            if(keys == null) {
                keys = parseKeys(line);
                log.info("Parsed keys from header: " + line);
            }
            else if(resolveKey == null) {
                asyncParseLine(slot, (JsonObject[]) array, line, keys);
                ++slot;
            }
            else {
                asyncParseLine(upsert, resolveKey, (List<JsonObject>) array,
                        line, keys);
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            Set<Long> records = upsert.get() ? upsertJsonString(array
                    .toString()) : importJsonString(Arrays
                    .toString((JsonObject[]) array));
            return records;
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        catch(Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * The delimiter that is used to split fields on each line.
     * 
     * @return
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
        }
        return element;
    }

    /**
     * Asynchronously process {@code line}.
     * 
     * @param upsert
     * @param resolveKey
     * @param objects
     * @param line
     * @param keys
     */
    private void asyncParseLine(final AtomicBoolean upsert,
            final String resolveKey, final List<JsonObject> objects,
            final String line, final String... keys) {
        executor.execute(new Runnable() {

            @Override
            public void run() {
                JsonObject object = parseLine(line, keys);
                if(object.has(resolveKey)) {
                    upsert.set(true);
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
                            synchronized (objects) {
                                objects.add(object);
                            }
                        }
                    }

                }
                else {
                    synchronized (objects) {
                        objects.add(object);
                    }
                }
                log.info("Processed {}", line);
            }
        });
    }

    /**
     * Asynchronously process {@code line}.
     * 
     * @param slot
     * @param objects
     * @param line
     * @param keys
     */
    private void asyncParseLine(final int slot, final JsonObject[] objects,
            final String line, final String... keys) {
        executor.execute(new Runnable() {

            @Override
            public void run() {
                JsonObject object = parseLine(line, keys);
                synchronized (objects) {
                    objects[slot] = object;
                }
                log.info("Processed {}", line);
            }

        });
    }

    /**
     * Parse the keys from the {@code line}. The delimiter can be specified by
     * the subclass in the {@link #delimiter()} method.
     * 
     * @param line
     * @return an array of keys
     */
    private final String[] parseKeys(String line) {
        return Strings
                .splitStringByDelimiterButRespectQuotes(line, delimiter());
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
        String[] toks = Strings.splitStringByDelimiterButRespectQuotes(line,
                delimiter());
        for (int i = 0; i < Math.min(keys.length, toks.length); i++) {
            if(StringUtils.isBlank(toks[i])) {
                continue;
            }
            JsonElement value = transformValue(keys[i], toks[i]);
            json.add(keys[i], value);
        }
        return json;
    }

}
