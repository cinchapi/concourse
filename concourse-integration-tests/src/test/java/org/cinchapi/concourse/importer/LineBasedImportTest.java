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
import java.util.Set;

import org.cinchapi.concourse.ConcourseIntegrationTest;
import org.cinchapi.concourse.testing.Variables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Convert.ResolvableLink;
import org.cinchapi.concourse.util.Resources;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Base unit test that validates the integrity of importing line based files
 * into Concourse.
 * 
 * @author Jeff Nelson
 */
public abstract class LineBasedImportTest extends ConcourseIntegrationTest {

    /**
     * The importer.
     */
    protected LineBasedImporter importer;

    @Override
    public void beforeEachTest() {
        importer = getImporter();
    }

    @Test
    public void testImport() {
        Collection<ImportResult> results = importer.importFile(Resources.get(
                "/" + getImportPath()).getFile());
        for (ImportResult result : results) {
            // Verify no errors
            Assert.assertEquals(0, result.getErrorCount());

            // Verify that fetch reads return all the appropriate values
            Multimap<String, String> data = result.getImportData();
            for (long record : result.getRecords()) {
                for (String key : data.keySet()) {
                    for (String value : data.get(key)) {
                        if(!Strings.isNullOrEmpty(value)) {
                            Object expected = Convert.stringToJava(value);
                            if(!(expected instanceof ResolvableLink)) {
                                Variables.register("key", key);
                                Variables.register("expected", expected);
                                Variables.register("record", record);
                                Set<Object> stored = client.select(key, record);
                                int count = 0;
                                for (Object obj : stored) {
                                    Variables.register("stored_" + count, obj);
                                }
                                Assert.assertTrue(stored.contains(expected));
                            }
                        }
                    }
                }
            }

            // Verify that find queries return all the appropriate records
            for (String key : data.keySet()) {
                for (String value : data.get(key)) {
                    if(!Strings.isNullOrEmpty(value)) {
                        Object stored = Convert.stringToJava(value);
                        if(!(stored instanceof ResolvableLink)) {
                            Assert.assertEquals(
                                    result.getRecords().size(),
                                    Sets.intersection(
                                            result.getRecords(),
                                            client.find(key, Operator.EQUALS,
                                                    stored)).size());
                        }
                    }
                }
            }
        }
    }

    /**
     * Return the importer to use in the test.
     * 
     * @return the importer
     */
    protected abstract LineBasedImporter getImporter();

    /**
     * Return the path of the file/directory to import.
     * 
     * @return the import path
     */
    protected abstract String getImportPath();

}
