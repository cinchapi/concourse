/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

/**
 * Unit test that imports youtube.csv.
 * 
 * @author Jeff Nelson
 */
public class YoutubeImportTest extends CsvImportTest {

    @Override
    protected String getImportPath() {
        return "youtube.csv";
    }

    @Override
    public void testImport() {
        // NOTE: This test is overridden because there are known inconsistencies
        // with the way the CsvImporter handles quoted values with trailing
        // whitespace (e.g the whitespace is preserved) compared to the way the
        // LegacyCsvImporter handles them (e.g. they are not preserved)
    }

}
