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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;

/**
 * This is the base class for bringing data from flat files into Concourse.
 * Implementing classes must provide logic for importing data from both
 * {@link #importFile(String) files} and {@link #importString(String) strings}.
 * A {@link #concourse client connection} to Concourse is provided so that the
 * implementation can choose the specific methods for optimally importing the
 * data from the file.
 * <p>
 * This framework does not mandate anything about the structure of the raw data
 * so it is possible to use this implementation to handle things like CSV files,
 * XML files, SQL dumps, email documents, etc in both generic and specific form.
 * </p>
 * <p>
 * An {@link Importer} is assumed to maintain state about the import process it
 * manages (i.e. it is possible, an expected that the importer will keep track
 * of things like which files it has imported or whether it has already
 * processed header information to provide metadata for the import). As a
 * result, importers are not safe for concurrent use by multiple threads.
 * </p>
 * 
 * @author jnelson
 */
@NotThreadSafe
public abstract class Importer {

    /**
     * The name of the key that contains the value for the
     * {@code --annotate-data-source} CLI option.
     */
    public static final String ANNOTATE_DATA_SOURCE_OPTION_NAME = "annotateDataSource";
    
    /**
     * The key that contains datasource annotations.
     */
    protected static final String DATA_SOURCE_ANNOTATION_KEY = "__datasource";

    /**
     * The connection to Concourse.
     */
    protected final Concourse concourse;

    /**
     * A map of dynamic parameters that may be passed in from the
     * {@link ImportCli}.
     */
    protected Map<String, String> params = Collections.emptyMap();

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected Importer(Concourse concourse) {
        this.concourse = concourse;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            // NOTE: We close the Importer in a shutdown hook so that the
            // process does not inflate the amount of time for importing

            @Override
            public void run() {
                close();
            }

        }));
    }

    /**
     * Close the importer and release any resources that it has open.
     */
    public final void close() {
        concourse.exit();
    }

    /**
     * Import the data from {@code file} into {@link Concourse} and return the
     * record ids where the data was imported.
     * 
     * @param file the path of the file to import; relative paths will be
     *            resolved against the {@code user.dir} system property. When
     *            using the {@link ImportCli}, prior
     *            to passing the path to this method, relative paths are
     *            converted to absolute paths by resolving against the directory
     *            from which the CLI process is launched
     * @return a {@link Set} that contains the ids of the records affected by
     *         the import
     */
    @Nullable
    public abstract Set<Long> importFile(String file);

    /**
     * Import {@code data} directly from a {@link String}, for instance input
     * from stdin.
     * 
     * @param data a string with data to import
     * @return a {@link Set} that contains the ids of the records affected by
     *         the import
     */
    @Nullable
    public abstract Set<Long> importString(String data);

    /**
     * Assign a collection of dynamic parameters to this {@link Importer} for
     * additional configuration.
     * 
     * @param params dynamic paramters, usually passed in from the
     *            {@link ImportCli}
     */
    public void setParams(Map<String, String> params) {
        this.params = params;
    }

}
