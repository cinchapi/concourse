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
package org.cinchapi.concourse.server.io;

import java.util.Set;

import org.cinchapi.concourse.ConcourseBaseTest;
import org.cinchapi.concourse.util.TestData;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests for the {@link FileSystem} utility class
 * 
 * @author jnelson
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

}
