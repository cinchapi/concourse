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

import java.util.Set;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.Constants;
import org.cinchapi.concourse.importer.util.Files;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.Convert;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import ch.qos.logback.classic.Logger;

/**
 * 
 * 
 * @author Jeff Nelson
 */
public class JsonImporter extends Importer {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected JsonImporter(Concourse concourse, Logger log) {
        super(concourse, log);
    }

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected JsonImporter(Concourse concourse) {
        super(concourse);
    }

    @Override
    public Set<Long> importFile(String file) {
        String json = Files.read(file);
        return upsertJsonString(json);
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
            Long record = Objects.firstNonNull((Long) Iterables.getOnlyElement(
                    data.get(Constants.JSON_RESERVED_IDENTIFIER_NAME), null),
                    Time.now());
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

}
