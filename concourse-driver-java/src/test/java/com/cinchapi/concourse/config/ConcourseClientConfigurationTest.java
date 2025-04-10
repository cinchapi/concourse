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
package com.cinchapi.concourse.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;

/**
 * Unit tests for the {@link ConcourseClientConfiguration} class.
 * 
 * @author Jeff Nelson
 */
public class ConcourseClientConfigurationTest extends ConcourseBaseTest {

    /**
     * Path to the prefs file that will be used within the test case.
     */
    private Path configPath = null;

    @Override
    protected void beforeEachTest() {
        try {
            configPath = Paths.get(java.nio.file.Files
                    .createTempFile(this.getClass().getName(), ".tmp")
                    .toString());
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Append a line to the test prefs file.
     * 
     * @param line the line to append
     */
    private void appendLine(String line) {
        try {
            Files.asCharSink(configPath.toFile(), StandardCharsets.UTF_8,
                    FileWriteMode.APPEND).write(line + System.lineSeparator());
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    @Override
    public void afterEachTest() {
        try {
            java.nio.file.Files.delete(configPath);
            configPath = null;
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    @Test
    public void testGetPasswordExplicitWhenAbset() {
        appendLine("username = foo");
        appendLine("host = localhost");
        ConcourseClientConfiguration prefs = ConcourseClientConfiguration
                .from(configPath);
        Assert.assertEquals(ConcourseClientConfiguration.NO_PASSWORD_DEFINED,
                prefs.getPasswordExplicit());
    }

    @Test
    public void testGetPasswordExplicitWhenPresent() {
        appendLine("username = foo");
        appendLine("host = localhost");
        appendLine("password = foofoo");
        ConcourseClientConfiguration prefs = ConcourseClientConfiguration
                .from(configPath);
        Assert.assertEquals("foofoo", new String(prefs.getPasswordExplicit()));
    }

    @Test
    public void testGetPortWhenWrittenAsString() {
        appendLine("port = \"1717\"");
        ConcourseClientConfiguration prefs = ConcourseClientConfiguration
                .from(configPath);
        Assert.assertEquals(1717, prefs.getPort());
    }

    @Test
    public void testGetPortWhenNotProvided() {
        ConcourseClientConfiguration prefs = ConcourseClientConfiguration
                .from(configPath);
        Assert.assertEquals(1717, prefs.getPort());
    }

    @Test
    public void testSet() {
        ConcourseClientConfiguration prefs = ConcourseClientConfiguration
                .from(configPath);
        prefs.setPort(9000);
        prefs = ConcourseClientConfiguration.from(configPath);
        Assert.assertEquals(9000, prefs.getPort());
    }

}
