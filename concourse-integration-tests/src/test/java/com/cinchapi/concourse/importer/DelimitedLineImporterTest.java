/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.test.ConcourseIntegrationTest;
import com.cinchapi.concourse.util.Resources;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base unit test that validates the integrity of importing delimited line based
 * data into Concourse.
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings("deprecation")
public abstract class DelimitedLineImporterTest
        extends ConcourseIntegrationTest {

    /**
     * The importer. A new instance of this is instantiated before each test.
     */
    protected DelimitedLineImporter importer;

    /**
     * A {@link com.cinchapi.concourse.importer.LineBasedImporter legacy}
     * importer that is used for
     * comparisons.
     */
    private LineBasedImporter legacy;

    @Override
    public void beforeEachTest() {
        importer = Reflection.newInstance(getImporterClass(), client);
        legacy = getLegacyImporter();
    }

    @Test
    public void testImport() {
        String file = Resources.get("/" + getImportPath()).getFile();
        Set<Long> records = importer.importFile(file);
        Set<Long> legacyRecords = legacy.importFile(file);
        Assert.assertEquals(
                Lists.newArrayList(client.select(legacyRecords).values()),
                Lists.newArrayList(client.select(records).values()));
    }

    @Test
    public void testTagSource() {
        Map<String, String> params = Maps.newLinkedHashMap(importer.params);
        params.put(Importer.ANNOTATE_DATA_SOURCE_OPTION_NAME, "true");
        importer.setParams(params);
        Set<Long> records = importer
                .importString("a,b" + System.lineSeparator() + "1,2");
        records.forEach(record -> {
            Assert.assertNotNull(
                    client.get(Importer.DATA_SOURCE_ANNOTATION_KEY, record));
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotSetHeaderAfterImport() {
        String file = Resources.get("/" + getImportPath()).getFile();
        importer.importFile(file);
        importer.parseHeader("a,b,c,d,e");
    }

    /**
     * Return the {@link Class} object for the {@link Importer} type used in the
     * test.
     * 
     * @return the Importer class
     */
    protected abstract Class<? extends DelimitedLineImporter> getImporterClass();

    /**
     * Return a {@link com.cinchapi.concourse.importer.LineBasedImporter legacy}
     * importer to use for validation of the new importer.
     *
     * @return the legacy Importer
     */
    protected abstract LineBasedImporter getLegacyImporter();

    /**
     * Return the path of the file/directory to import.
     * 
     * @return the import path
     */
    protected abstract String getImportPath();

}
