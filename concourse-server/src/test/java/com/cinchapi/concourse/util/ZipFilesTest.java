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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ZipFiles} utility class.
 *
 * @author Jeff Nelson
 */
public class ZipFilesTest {

    @Test(expected = IllegalArgumentException.class)
    public void testCannotExtractZipContainingEntryWithPathTraversalCharacters()
            throws IOException { // CON-626
        String zip = Resources.getAbsolutePath("/evil.zip");
        String parent = FileOps.tempDir("zip");
        String dest = Paths.get(parent).resolve("target").toAbsolutePath()
                .toString();
        ZipFiles.unzip(zip, dest);
        Assert.assertEquals(1, Files.list(Paths.get(dest)).count());
        Assert.assertEquals(1, Files.list(Paths.get(parent)).count());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotExtractZipFileToNonDestination() {
        String zip = Resources.getAbsolutePath("/good.zip");
        String dest = FileOps.tempFile();
        ZipFiles.unzip(zip, dest);
    }

    @Test
    public void testGetEntryContentUtf8() {
        String zip = Resources.getAbsolutePath("/good.zip");
        String content = ZipFiles.getEntryContentUtf8(zip, "file.txt");
        Assert.assertEquals("Hello World", content);
    }

    @Test
    public void testUnzip() throws IOException {
        String zip = Resources.getAbsolutePath("/good.zip");
        String parent = FileOps.tempDir("zip");
        String dest = Paths.get(parent).resolve("target").toAbsolutePath()
                .toString();
        ZipFiles.unzip(zip, dest);
        Assert.assertEquals(1, Files.list(Paths.get(dest)).count());
        Assert.assertEquals(1, Files.list(Paths.get(parent)).count());
    }

}
