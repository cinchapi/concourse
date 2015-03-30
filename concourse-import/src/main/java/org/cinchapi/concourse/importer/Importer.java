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

import java.util.Collection;
import javax.annotation.Nullable;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.TransactionException;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Convert.ResolvableLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

/**
 * Base implementation of the {@link Importer} interface that handles the work
 * of importing data into Concourse. The subclass that extends this
 * implementation only needs to worry about implementing the
 * {@link #handleFileImport(String)} method and converting each group of raw
 * data (i.e. a line in csv file) into a {@link Multimap} that is passed to the
 * {@link #importGroup(Multimap, String)} method.
 * <p>
 * This implementation does not mandate anything about the structure of the raw
 * data other than the assumption that it can be transformed into one or more
 * multi-mappings of keys to values (called a <strong>group</strong>). All data
 * in a group is imported into the appropriate record(s) together. Since this
 * requirement is very lightweight, it is possible to extend this implementation
 * to handle things like CSV files, XML files, SQL dumps, email documents, etc.
 * </p>
 * <h2>Import into new record</h2>
 * <p>
 * By default, all the data in a group is converted into one new record when
 * using the {@link #importGroup(Concourse, Multimap)} method.
 * </p>
 * <h2>Import into existing record(s)</h2>
 * <p>
 * It is possible to import all the data in a group into one or more existing
 * records by specifying a <strong>resolveKey</strong> in the
 * {@link #doImport(Concourse, Multimap, String)} method.
 * </p>
 * <p>
 * For each group, the importer will find all the records that have at least one
 * of the same values mapped from {@code resolveKey} as defined in the group
 * data. The importer will then import all the group data into all of those
 * record.
 * </p>
 * <p>
 * <strong>NOTE:</strong> It is not possible to specify the Primary Key of a
 * record as the resolveKey. The Primary Key is metadata which isn't necessarily
 * known to the raw data. Therefore, we prefer that record resolution happens in
 * terms that are known by the raw data.
 * </p>
 * <h2>Specifying links in raw data</h2>
 * <p>
 * It is possible to link the record into which group data is imported to
 * another existing record. There is a special format for specifying
 * <strong>resolvable links</strong> in raw data. The subclass can convert the
 * raw data to that format by calling the
 * {@link Convert#stringToResolvableLinkSpecification(String, String)} on the
 * raw data before converting it to a multimap.
 * </p>
 * <p>
 * When the importer encounters a {@link ResolvableLink}, similar to a
 * resolveKey, it finds all the records who have the value for the specified key
 * and links the record into which the group data is imported to all those
 * records.
 * </p>
 * <h4>Example</h4>
 * <table>
 * <tr>
 * <th>account_number</th>
 * <th>customer</th>
 * <th>account_type</th>
 * </tr>
 * <tr>
 * <td>12345</td>
 * <td>@&lt;customer_id&gt;@678@&lt;customer_id&gt;@</td>
 * <td>SAVINGS</td>
 * </tr>
 * </table>
 * <p>
 * Lets assume in this scenario, I've imported customer data. Now, I want to
 * link the "customer" key in each account record that is created to the
 * previously created customer records. By specifying the raw customer value as
 * 
 * {@code @<customer_id>@678@<customer_id>@} I am saying that the system should
 * find all the records where customer_id is equal to 678 and link the
 * "customer" key in the account record that is being created to all those
 * records.
 * <p>
 * 
 * @author jnelson
 */
public abstract class Importer {

    /**
     * A Logger that is available for the subclass to log helpful messages.
     */
    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     * The connection to Concourse.
     */
    private final Concourse concourse;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    protected Importer(Concourse concourse) {
        this.concourse = concourse;
    }

    /**
     * Import the data contained in {@code file} into {@link Concourse}.
     * 
     * @param file
     * @return a collection of {@link ImportResult} objects that describes the
     *         records created/affected from the import and whether any errors
     *         occurred
     */
    public final Collection<ImportResult> importFile(String file) {
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
    public final Collection<ImportResult> importFile(String file,
            @Nullable String resolveKey) {
        concourse.stage();
        try {
            Collection<ImportResult> results = splitAndImport(concourse, file,
                    resolveKey);
            if(concourse.commit()) {
                return results;
            }
        }
        catch (TransactionException e) {
            concourse.abort();
        }
        throw new RuntimeException("Failed to import " + file);
    }

    /**
     * Split the {@code file} as appropriate and import the data into
     * {@code concourse} using the {@code resolveKe}, if it is not {@code null}.
     * 
     * @param concourse
     * @param file
     * @param resolveKey
     * @return the results of the import
     */
    protected abstract Collection<ImportResult> splitAndImport(
            Concourse concourse, String file, @Nullable String resolveKey);

}
