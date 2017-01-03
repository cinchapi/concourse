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

import java.text.MessageFormat;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.concourse.util.Strings;
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
    public void testImport() {/* noop */}

    @Test
    @Ignore
    public void testResolveKey() {
        String file0 = Resources.get("/resolve_key_0.csv").getFile();
        String file1 = Resources.get("/resolve_key_1.csv").getFile();
        String resolveKey = "ipeds_id";
        importer.importFile(file0, resolveKey);
        Set<Long> records = importer.importFile(file1,
                resolveKey);
        List<String> lines = FileOps.readLines(file1);
        int i = 0;
        for(String line : lines){
            if(i > 0){
                String[] toks = Strings.splitStringByDelimiterButRespectQuotes(line, ",");
                String ipedsId = toks[0];
                Long record = Iterables.get(records, i);
                Set<Long> matching = client.find(MessageFormat.format("{} = {}", resolveKey, ipedsId));
                Assert.assertEquals(1, matching.size());
                Assert.assertEquals(record, Iterables.<Long> getOnlyElement(matching));
            }
            ++i;
        }

    }
    
    @Test
    @Ignore
    public void testCannotSetHeaderAfterImport(){}

}
