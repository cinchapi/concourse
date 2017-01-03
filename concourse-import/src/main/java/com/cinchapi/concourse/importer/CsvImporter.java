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

import com.cinchapi.concourse.Concourse;

/**
 * <p>
 * An {@link Importer} that can handle generic CSV files that have header
 * information in the first line. It is advisable to extend this class for CSV
 * data that has special requirements. This class makes some general assumptions
 * that can be configured in a subclass:
 * </p>
 * <h2>Header</h2>
 * <p>
 * It is assumed that the first line of a CSV file contains the header. If that
 * is not the case, the subclass can return an ordered array of header keys from
 * the {@link #header()} method.
 * </p>
 * <h2>Delimiter</h2>
 * <p>
 * It is assumed that values in a line are comma separated. If that is not the
 * case, the subclass can specify a different rule in the {@link #delimiter()}
 * method.
 * </p>
 * <h2>Transforming Values</h2>
 * <p>
 * It is assumed that the original file data is correct. If that is not the
 * case, the subclass can selectively transform some values in the
 * {@link #transformValue(String, String)} method. For example, it might be
 * desirable specify link resolution, compact data or normalize data.
 * </p>
 * 
 * @author Jeff Nelson
 */
public class CsvImporter extends DelimitedLineImporter {

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public CsvImporter(Concourse concourse) {
        super(concourse);
    }

    @Override
    protected char delimiter() {
        return ',';
    }

}
