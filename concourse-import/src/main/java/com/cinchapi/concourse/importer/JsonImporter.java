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
import java.util.Set;

import org.slf4j.Logger;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Constants;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.FileOps;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * An {@link Importer} that uses Concourse's {@link Concourse#insert(String)
 * bulk JSON insert} functionality for more efficient imports.
 * <p>
 * This class can be used as a standalone importer for JSON files and it can be
 * extended to provide efficient inserts of other file formats. For example, an
 * extension can parse line based files into a JSON intermediate format that is
 * handled by the logic defined in this class.
 * </p>
 *
 * @author Jeff Nelson
 */
public class JsonImporter extends Importer {

    protected final Logger log;

    /**
     * Construct a new instance.
     *
     * @param concourse
     */
    public JsonImporter(Concourse concourse) {
        super(concourse);
        this.log = null;
    }

    /**
     * Construct a new instance.
     *
     * @param concourse
     */
    protected JsonImporter(Concourse concourse, Logger log) {
        super(concourse);
        this.log = log;
    }

    @Override
    public Set<Long> importFile(String file) {
        Set<Long> records = importString(FileOps.read(file));
        if(Boolean.parseBoolean(params.getOrDefault(
                Importer.ANNOTATE_DATA_SOURCE_OPTION_NAME, "false"))) {
            String filename = Paths.get(file).getFileName().toString();
            concourse.add(DATA_SOURCE_ANNOTATION_KEY, filename, records);
        }
        return records;
    }

    /**
     * Given a string of JSON data, insert it into Concourse.
     *
     * @param json
     * @return the records that were affected by the import
     */
    protected Set<Long> importJsonString(String json) {
        return concourse.insert(json);
    }

    /**
     * Given a string of JSON data, upsert it into Concourse.
     *
     * @param json
     * @return the records that were affected by the import
     */
    protected Set<Long> upsertJsonString(String json) {
        // TODO call concourse.upsert(json) when method is ready
        // NOTE: The following implementation is very inefficient, but will
        // suffice until the upsert functionality is available
        Set<Long> records = Sets.newHashSet();
        for (Multimap<String, Object> data : Convert.anyJsonToJava(json)) {
            Long record = MoreObjects
                    .firstNonNull((Long) Iterables.getOnlyElement(
                            data.get(Constants.JSON_RESERVED_IDENTIFIER_NAME),
                            null), Time.now());
            data.removeAll(Constants.JSON_RESERVED_IDENTIFIER_NAME);
            for (String key : data.keySet()) {
                for (Object value : data.get(key)) {
                    concourse.add(key, value, record);
                }
            }
            records.add(record);
        }
        return records;
    }

    @Override
    public Set<Long> importString(String json) {
        return concourse.insert(json);
    }

}
