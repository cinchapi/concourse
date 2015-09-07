/*
 * Copyright (c) 2013-2015 Cinchapi Inc.
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

import java.util.List;
import java.util.Set;

import org.cinchapi.concourse.test.ConcourseIntegrationTest;
import org.cinchapi.concourse.util.FileOps;
import org.cinchapi.concourse.test.Variables;
import org.cinchapi.concourse.util.Convert;
import org.cinchapi.concourse.util.Convert.ResolvableLink;
import org.cinchapi.concourse.util.QuoteAwareStringSplitter;
import org.cinchapi.concourse.util.Resources;
import org.cinchapi.concourse.util.Strings;
import org.cinchapi.concourse.util.TLists;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
        String file = Resources.get("/" + getImportPath()).getFile();
        Set<Long> records = importer.importFile(file);
        List<String> lines = FileOps.readLines(file);
        int size = 0;
        String[] keys = null;
        for (String line : lines) {
            Variables.register("line", line);
            long record = Iterables.get(records, size);
            Variables.register("record", record);
            if(keys == null) {
                Variables.register("keys", line);
                if(importer.useOptimizedSplitPath) {
                    List<String> keysList = Lists.newArrayList();
                    QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(
                            line, ',');
                    while (it.hasNext()) {
                        keysList.add(it.next().trim());
                    }
                    keys = TLists.toArrayCasted(keysList, String.class);
                }
                else {
                    keys = Strings.splitStringByDelimiterButRespectQuotes(line,
                            ",");
                }
            }
            else {
                String toks[] = null;
                if(importer.useOptimizedSplitPath) {
                    List<String> toksList = Lists.newArrayList();
                    QuoteAwareStringSplitter it = new QuoteAwareStringSplitter(
                            line, ',');
                    while (it.hasNext()) {
                        toksList.add(it.next());
                    }
                    toks = TLists.toArrayCasted(toksList, String.class);
                }
                else {
                    toks = Strings.splitStringByDelimiterButRespectQuotes(line,
                            ",");
                }
                for (int i = 0; i < Math.min(keys.length, toks.length); ++i) {
                    String key = keys[i];
                    String value = toks[i];
                    if(!com.google.common.base.Strings.isNullOrEmpty(value)) {
                        Object expected = Convert.stringToJava(value);
                        Object actual = client.get(key, record);
                        Variables.register("key", key);
                        Variables.register("raw", value);
                        Variables.register("expected", expected);
                        Assert.assertNotNull(actual);
                        Variables.register("actual", actual);
                        if(!(expected instanceof ResolvableLink)) {
                            Assert.assertTrue(client.verify(key, expected,
                                    record));
                        }
                    }
                }
                size += 1;
            }
        }
        Assert.assertEquals(records.size(), size); // account for header
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
