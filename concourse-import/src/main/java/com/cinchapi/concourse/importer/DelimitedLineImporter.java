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

import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.importer.util.Importables;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.QuoteAwareStringSplitter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * An {@link Importer} that splits each line of input by a {@link #delimiter()}
 * to produce key/value pairs for import. Each line is imported entirely into at
 * least one record in Concourse.
 * 
 * @author Jeff Nelson
 */
public abstract class DelimitedLineImporter extends Importer
        implements Headered {

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
        this.transformer = transformer();
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
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (String line : lines) {
            int length = sb.length();
            Importables.delimitedStringToJsonObject(line, resolveKey, delimiter,
                    header, transformer, sb);
            if(sb.length() > length) {
                sb.append(',');
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']');
        Set<Long> records = concourse.insert(sb.toString());
        if(Boolean.parseBoolean(params.getOrDefault(
                Importer.ANNOTATE_DATA_SOURCE_OPTION_NAME, "false"))) {
            String filename = Paths.get(file).getFileName().toString();
            concourse.add(DATA_SOURCE_ANNOTATION_KEY, filename, records);
        }
        return records;
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
        String json = Importables.delimitedStringToJsonArray(lines, resolveKey,
                delimiter, header, transformer);
        return concourse.insert(json);
    }

    @Override
    public final void parseHeader(String line) {
        Preconditions.checkState(header.isEmpty(),
                "Header has been set already");
        QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(line,
                delimiter);
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
     * Return a {@link Transformer} to potentially alter key/value pairs seen by
     * this importer.
     * 
     * @return the transformer
     */
    protected Transformer transformer() {
        return null;
    }

}
