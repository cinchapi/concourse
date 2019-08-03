/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.util.Resources;
import com.cinchapi.etl.Transformation;
import com.cinchapi.etl.Transformer;
import com.cinchapi.etl.Transformers;

/**
 * Unit test for importing data with timestamps.
 *
 * @author Jeff Nelson
 */
public class TimestampImportTest extends CsvImportTest {

    @Override
    protected String getImportPath() {
        return "timestamp.csv";
    }

    @Test
    public void testTimestampImport() {
        String file = Resources.get("/" + getImportPath()).getFile();
        importer = new CsvImporter(client) {

            @Override
            protected Transformer transformer() {
                return Transformers.compose((key, value) -> {
                    if(key.equals("Date")) {
                        return Transformation.to(key,
                                AnyStrings.format("|{}|MM/dd/yyyy|", value));
                    }
                    else {
                        return null;
                    }
                });
            }

        };
        Set<Long> records = importer.importFile(file);
        client.get("Date", records).forEach((record, date) -> {
            Assert.assertEquals(Timestamp.class, date.getClass());
        });
    }

}
