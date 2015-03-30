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

import java.util.Collection;
import java.util.Set;

import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Resources;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Unit tests to validate the resolve key functionality.
 * 
 * @author Jeff Nelson
 */
public class ResolveKeyTest extends CsvImportTest {

    @Override
    protected String getImportPath() {
        return null;
    }
    
    @Override
    @Test
    @Ignore
    public void testImport(){/*noop*/}
    
    @Test
    public void testResolveKey() {
        String file0 = Resources.get("/resolve_key_0.csv").getFile();
        String file1 = Resources.get("/resolve_key_1.csv").getFile();
        String resolveKey = "ipeds_id";
        importer.importFile(file0, resolveKey);
        Collection<ImportResult> results = importer.importFile(file1,
                resolveKey);
        for (ImportResult result : results) {
            Object value = Convert.stringToJava(Iterables.getOnlyElement(result
                    .getImportData().get(resolveKey)));
            Set<Long> records = client.find(resolveKey, Operator.EQUALS, value);
            Assert.assertEquals(1, records.size());
        }

    }

}
