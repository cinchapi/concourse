/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * An ImportResult contains information about the nature of an attempt to import
 * data into one or more records (i.e. did the import succeed, etc).
 *
 * @author Jeff Nelson
 */
@Immutable
public final class ImportResult {

    // NOTE: This class does not define hashCode(), equals() or toString()
    // because the defaults are the desired behaviour

    /**
     * Return an {@link ImportResult} that describes what occurs during an
     * attempt to import {@code importData} into {@code record}.
     *
     * @param importData
     * @param records
     * @return the ImportRecordResult
     */
    public static ImportResult newImportResult(
            Multimap<String, String> importData, Set<Long> records) {
        return new ImportResult(importData, records);
    }

    /**
     * A collection of strings describing errors that occurred during the
     * import.
     */
    private final List<String> errors = Lists.newArrayList();

    /**
     * The raw data that was used in the attempted import.
     */
    private final Multimap<String, String> importData;

    /**
     * The records into which the data was imported.
     */
    private final Set<Long> records;

    /**
     * Construct a new instance.
     *
     * @param importData
     */
    private ImportResult(Multimap<String, String> importData, Set<Long> records) {
        this.importData = importData;
        this.records = records;
    }

    /**
     * Add an indication that an error has occurred, described by
     * {@code message}.
     *
     * @param message
     */
    protected void addError(String message) {
        errors.add(message);
    }

    /**
     * Return the number of errors that occurred during the import.
     *
     * @return the error count
     */
    public int getErrorCount() {
        return errors.size();
    }

    /**
     * Return the raw data that was used in the import.
     *
     * @return the import data
     */
    public Multimap<String, String> getImportData() {
        return importData;
    }

    /**
     * Return the records into which the data was imported.
     *
     * @return the records
     */
    public Set<Long> getRecords() {
        return records;
    }

    /**
     * Return {@code true} if the import encountered errors.
     *
     * @return {@code true} if there were errors
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

}
