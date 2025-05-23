/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.common.base.QuoteAwareStringSplitter;
import com.cinchapi.common.base.SplitOption;
import com.cinchapi.common.base.StringSplitter;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.etl.Strainer;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.etl.Transformer;
import com.cinchapi.etl.Transformers;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;

/**
 * An {@link Importer} that splits each line of input by a {@link #delimiter()}
 * to produce key/value pairs for import. Each line is imported entirely into at
 * least one record in Concourse.
 * 
 * @author Jeff Nelson
 */
public abstract class DelimitedLineImporter extends Importer implements
        Headered {

    /**
     * The character on which each line in the text is split to generate tokens.
     * This is set by the subclass using the {@link #delimiter()} method.
     */
    private final char delimiter;

    /**
     * A collection of column/key names that map to each of the delimited tokens
     * in a line, respectively.
     */
    private final List<String> header;

    /**
     * The {@link Transformer} that possible alters key/value pairs prior to
     * import. This is set by the subclass using the {@link #transformer()}
     * method.
     */
    @Nullable
    private final Transformer transformer;

    /**
     * Construct a new instance.
     * 
     * @param concourse the connection to Concourse to use for importing
     */
    protected DelimitedLineImporter(Concourse concourse) {
        super(concourse);
        this.delimiter = delimiter();
        // Backwards compatibility: The strings that are read from the
        // file/stream must be converted to their assumed java type.
        this.transformer = Transformers.composeForEach(
                Transformers.valueStringToJava(),
                Transformers.valueRemoveIf(
                        Empty.is(String.class, StringUtils::isBlank)),
                MoreObjects.firstNonNull(transformer(), Transformers.noOp()));
        this.header = header();
    }

    @Override
    public final Set<Long> importFile(String file) {
        return importFile(file, null);
    }

    /**
     * Import all of the lines from {@code file} into Concourse and determine
     * the appropriate destination record(s) for each line using the
     * {@code resolveKey}.
     * 
     * @param file the path to the file to import
     * @param resolveKey the key to use when resolving one or more existing
     *            records into which to import a line. When using a
     *            {@code resolveKey}, the Importer instructs Concourse to find
     *            all the records that contain the same value for the key that
     *            exists in the line and import the rest of the data in the line
     *            into those records
     * @return the records into which the data is imported
     */
    public final Set<Long> importFile(String file,
            @Nullable String resolveKey) {
        List<String> lines = FileOps.readLines(file);
        String source = Paths.get(file).getFileName().toString();
        return importLines(lines, resolveKey, source);
    }

    @Override
    public Set<Long> importString(String data) {
        return importString(data, null);
    }

    /**
     * Import all the {@code lines} into Concourse and determine the appropriate
     * destination record(s) for each line using the {@code resolveKey}.
     * 
     * @param lines the text to import
     * @param resolveKey the key to use when resolving one or more existing
     *            records into which to import a line. When using a
     *            {@code resolveKey}, the Importer instructs Concourse to find
     *            all the records that contain the same value for the key that
     *            exists in the line and import the rest of the data in the line
     *            into those records
     * @return the records into which the data is imported
     */
    public final Set<Long> importString(String lines,
            @Nullable String resolveKey) {
        List<String> _lines = Lists.newArrayList(lines.split("\\r?\\n"));
        // Assume that the lines come from stdin and hash them to generate a
        // generic source id
        String source = Hashing.murmur3_128()
                .hashString(lines, StandardCharsets.UTF_8).toString();
        return importLines(_lines, resolveKey, source);
    }

    @Override
    public final void parseHeader(String line) {
        Preconditions.checkState(header.isEmpty(),
                "Header has been set already");
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(line,
                delimiter, SplitOption.TRIM_WHITESPACE);
        while (it.hasNext()) {
            header.add(it.next());
        }
    }

    /**
     * The delimiter that is used to split field on each line.
     * 
     * @return the delimiter
     */
    protected abstract char delimiter();

    /**
     * Provide an ordered list of header columns/keys if they are not provided
     * in the first line that is processed by this importer.
     * 
     * @return the header
     */
    protected List<String> header() {
        return Lists.newArrayList();
    }

    /**
     * Process the {@code lines} and import the parsed objects into Concourse.
     * 
     * @param lines
     * @param resolveKey
     * @param source
     * @return the ids of the records into which the objects are imported
     */
    protected final Set<Long> importLines(List<String> lines,
            @Nullable String resolveKey, @Nullable String source) {
        // TODO: process resolve key
        List<Multimap<String, Object>> objects = Lists.newArrayList();
        lines.forEach(line -> {
            if(header == null || header.isEmpty()) {
                parseHeader(line);
            }
            else {
                Multimap<String, Object> object = parseObject(line);
                if(Boolean.parseBoolean(params.getOrDefault(
                        Importer.ANNOTATE_DATA_SOURCE_OPTION_NAME, "false"))) {
                    object.put(DATA_SOURCE_ANNOTATION_KEY, source);
                }
                objects.add(object);
            }
        });
        if(!objects.isEmpty()) {
            return concourse.insert(objects);
        }
        else {
            return ImmutableSet.of();
        }
    }

    /**
     * Transform the delimited line into an object where each field is mapped as
     * a value from the analogous key in the {@link #header}. Each key/value
     * mapping is subject to processing by the {@link #transformer}.
     * 
     * @param line
     * @return the parsed object
     */
    protected Multimap<String, Object> parseObject(String line) {
        StringSplitter it = new QuoteAwareStringSplitter(line, delimiter,
                SplitOption.TRIM_WHITESPACE);
        Multimap<String, Object> object = LinkedHashMultimap.create();
        Strainer strainer = new Strainer(
                (key, value) -> object.put(key, value));
        int col = 0;
        while (it.hasNext()) {
            String key = header.get(col++);
            String value = it.next();
            Map<String, Object> transformed = transformer.transform(key,
                    (Object) value);
            if(transformed != null) {
                strainer.process(transformed);
            }
            else {
                strainer.process(key, value);
            }
        }
        return object;
    }

    /**
     * Return a {@link Transformer} to potentially alter key/value pairs seen by
     * this importer.
     * 
     * @return the transformer
     */
    protected Transformer transformer() {
        return null;
    }

}
