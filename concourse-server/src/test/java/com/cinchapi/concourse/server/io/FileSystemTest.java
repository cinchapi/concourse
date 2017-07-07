/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.test.ConcourseBaseTest;
import com.cinchapi.concourse.util.TestData;
import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link FileSystem} utility class
 * 
 * @author Jeff Nelson
 */
public class FileSystemTest extends ConcourseBaseTest {

    @Test
    public void testGetSubDirs() {
        String dir = TestData.getTemporaryTestDir();
        Set<String> subdirs = Sets.newHashSet();
        for (int i = 0; i < TestData.getScaleCount(); i++) {
            String subdir = TestData.getString();
            FileSystem.mkdirs(FileSystem.makePath(dir, subdir));
            subdirs.add(subdir);
        }
        Assert.assertEquals(subdirs, FileSystem.getSubDirs(dir));
    }
    
    @Test
    public void testExpandPath() {
        // Test paths starting with "." and containing ".."
        String path = "./bin/../src/resources";
        Assert.assertEquals("correct_path_1", FileSystem.getWorkingDirectory()
                + "/src/resources", FileSystem.expandPath(path));
        // Test paths with multiple ".."
        path = "bin/../bin/../src/resources";
        Assert.assertEquals("correct_path_2", FileSystem.getWorkingDirectory()
                + "/src/resources", FileSystem.expandPath(path));
        // Test Unix home tilde.
        path = "~";
        Assert.assertEquals("correct_path_3", FileSystem.getUserHome(),
                FileSystem.expandPath(path));
        // Test path with multiple consecutive forward slash.
        path = "./src/resources/////////////////";
        Assert.assertEquals("correct_path_4", FileSystem.getWorkingDirectory()
                + "/src/resources", FileSystem.expandPath(path));
        // Test path with multiple consecutive forward slash, followed by
        // multiple "..".
        path = "./src/resources/////////////////../..";
        Assert.assertEquals("correct_path_5",
                FileSystem.getWorkingDirectory(), FileSystem.expandPath(path));
    }

}
