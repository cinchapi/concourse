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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.Concourse;

/**
 * The legacy implementation of {@link CsvImporter} that extends
 * {@link LineBasedImporter}. This shouldn't be used anymore. It only still
 * exists for debugging and performance comparisons.
 * 
 * @author Jeff Nelson
 */
@Deprecated
public class LegacyCsvImporter extends LineBasedImporter {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public LegacyCsvImporter(Concourse concourse) {
        this(concourse, LoggerFactory.getLogger(LegacyCsvImporter.class));
    }

    /**
     * Construct a new instance.
     * 
     * @param concourse
     * @param log
     */
    protected LegacyCsvImporter(Concourse concourse, Logger log) {
        super(concourse, log);
    }

    @Override
    protected String delimiter() {
        return ",";
    }

    @Override
    protected void validateFileFormat(String line)
            throws IllegalArgumentException {
        if(line.startsWith("<") && line.endsWith(">")) {
            throw new IllegalArgumentException(
                    "CSV file cannot be imported when the "
                            + "first line starts and ends with angle brackets");
        }
    }

}
