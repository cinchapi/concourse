/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
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
