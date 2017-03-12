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
package com.cinchapi.concourse.config;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

/**
 * Unit tests for the {@link ConcourseClientPreferences} class.
 * 
 * @author Jeff Nelson
 */
public class ConcourseClientPreferencesTest extends ConcourseBaseTest {

    /**
     * Path to the prefs file that will be used within the test case.
     */
    private String prefsPath = null;

    @Override
    protected void beforeEachTest() {
        try {
            prefsPath = java.nio.file.Files.createTempFile(
                    this.getClass().getName(), ".tmp").toString();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Append a line to the test prefs file.
     * 
     * @param line the line to append
     */
    private void appendLine(String line) {
        try {
            Files.append(line + System.lineSeparator(), new File(prefsPath),
                    StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void afterEachTest() {
        try {
            java.nio.file.Files.delete(Paths.get(prefsPath));
            prefsPath = null;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testGetPasswordExplicitWhenAbset() {
        appendLine("username = foo");
        appendLine("host = localhost");
        ConcourseClientPreferences prefs = ConcourseClientPreferences
                .open(prefsPath);
        Assert.assertEquals(ConcourseClientPreferences.NO_PASSWORD_DEFINED,
                prefs.getPasswordExplicit());
    }

    @Test
    public void testGetPasswordExplicitWhenPresent() {
        appendLine("username = foo");
        appendLine("host = localhost");
        appendLine("password = foofoo");
        ConcourseClientPreferences prefs = ConcourseClientPreferences
                .open(prefsPath);
        Assert.assertEquals("foofoo", new String(prefs.getPasswordExplicit()));
    }

}
