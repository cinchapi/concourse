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

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.importer.util.Files;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Convert.ResolvableLink;
import org.cinchapi.concourse.util.Strings;

import com.google.common.base.Throwables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * An {@link Importer} that handles data from a file that can be delimited into
 * one or more lines. Each line is considered a single group of data that can be
 * converted to a multimap and imported in one or more records in Concourse.
 * 
 * @author Jeff Nelson
 */
public abstract class LineBasedImporter extends Importer {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected LineBasedImporter(Concourse concourse) {
        super(concourse);
    }

    /**
     * Parse the data from {@code line} into a multimap from key (header) to
     * value.
     * 
     * @param line
     * @param keys
     * @return the line data
     */
    public final Multimap<String, String> parseLine(String line, String... keys) {
        Multimap<String, String> data = LinkedHashMultimap.create();
        String[] toks = Strings.splitStringByDelimiterButRespectQuotes(line,
                delimiter());
        for (int i = 0; i < Math.min(keys.length, toks.length); i++) {
            for (String value : transformValue(keys[i], toks[i])) {
                data.put(keys[i], value);
            }
        }
        return data;
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
     * Import a single group of {@code data} (i.e. a line in a csv file) into
     * {@code concourse}.
     * <p>
     * If {@code resolveKey} is specified, it is possible that the {@code data}
     * will be added to more than one existing record. It is guaranteed that an
     * attempt will be made to add the data to at least one (possibly) new
     * record.
     * </p>
     * 
     * @param data
     * @param resolveKey
     * @return an {@link ImportResult} object that describes the records
     *         created/affected from the import and whether any errors occurred.
     */
    protected final ImportResult import0(Concourse concourse,
            Multimap<String, String> data, @Nullable String resolveKey) {
        // Determine import record(s)
        Set<Long> records = Sets.newHashSet();
        for (String resolveValue : data.get(resolveKey)) {
            records = Sets.union(
                    records,
                    concourse.find(resolveKey, Operator.EQUALS,
                            Convert.stringToJava(resolveValue)));
            records = Sets.newHashSet(records); // must make copy because
                                                // previous method returns
                                                // immutable view
        }
        if(records.isEmpty()) {
            records.add(Time.now());
        }
        // Iterate through the data and add it to Concourse
        ImportResult result = ImportResult.newImportResult(data, records);
        for (String key : data.keySet()) {
            for (String rawValue : data.get(key)) {
                if(!com.google.common.base.Strings.isNullOrEmpty(rawValue)) { // do not waste time
                                                                              // sending
                                                                              // empty
                                                                              // values
                                                                              // over
                                                                              // the
                                                                              // wire
                    Object convertedValue = Convert.stringToJava(rawValue);
                    List<Object> values = Lists.newArrayList();
                    if(convertedValue instanceof ResolvableLink) {
                        // Find all the records that resolve and create a
                        // Link to those records.
                        for (long record : concourse.find(
                                ((ResolvableLink) convertedValue).getKey(),
                                Operator.EQUALS,
                                ((ResolvableLink) convertedValue).getValue())) {
                            values.add(Link.to(record));
                        }
                    }
                    else {
                        values.add(convertedValue);
                    }
                    for (long record : records) {
                        for (Object value : values) {
                            if(!concourse.add(key, value, record)) {
                                result.addError(MessageFormat.format(
                                        "Could not import {0} AS {1} IN {2}",
                                        key, value, record));
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Parse the keys from the {@code line}.
     * 
     * @param line
     * @return an array of keys
     */
    protected String[] parseKeys(String line) {
        return Strings
                .splitStringByDelimiterButRespectQuotes(line, delimiter());
    }

    @Override
    protected Collection<ImportResult> splitAndImport(Concourse concourse,
            String file, String resolveKey) {
        List<ImportResult> results = Lists.newArrayList();
        String[] keys = header();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(
                    Files.expandPath(file)));
            String line;
            while ((line = reader.readLine()) != null) {
                line = cleanup(line);
                if(keys == null) {
                    keys = parseKeys(line);
                    log.info("Processed header: " + line);
                }
                else {
                    Multimap<String, String> data = parseLine(line, keys);
                    ImportResult result = import0(concourse, data, resolveKey);
                    results.add(result);
                    log.info(MessageFormat
                            .format("Imported {0} into record(s) {1} with {2} error(s)",
                                    line, result.getRecords(),
                                    result.getErrorCount()));
                }
            }
            reader.close();
            return results;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }      
    }

    /**
     * This method allows the subclass to define dynamic intermediary
     * transformations to data to better prepare it for import. This method is
     * called before the raw string data is converted to a Java object. There
     * are several instances for which the subclass should use this method:
     * <p>
     * <h2>Specifying Link Resolution</h2>
     * The importer will convert raw data of the form
     * <code>@&lt;key&gt;@value@&lt;key&gt;@</code> into a Link to all the
     * records where key equals value in Concourse. For this purpose, the
     * subclass can convert the raw value to this form using the
     * {@link #transformValueToResolvableLink(String, String)} method.
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
     * @return the transformed value
     */
    protected String[] transformValue(String key, String value) {
        return new String[] { value };
    }

    /**
     * Cleanup the line before processing it.
     * 
     * @param line
     * @return the cleaned up line
     */
    private String cleanup(String line) {
        return line.replaceAll(delimiter() + " ", delimiter());
    }

}
