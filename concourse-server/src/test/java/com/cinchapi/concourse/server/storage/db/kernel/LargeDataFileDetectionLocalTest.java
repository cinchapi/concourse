/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.junit.Assert;

import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.temp.Write;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.FileOps;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.concourse.util.Resources;

/**
 * Local test for large lock detection
 *
 * @author Jeff Nelson
 */
public class LargeDataFileDetectionLocalTest {

    // NOTE: This is designed to be run locally in an IDE or the command line
    // where the heap size can be set sufficiently large

    public static void main(String... args) throws SegmentLoadingException {
        String directory = FileOps.tempDir("test");
        Segment segment = Segment.create();
        Assert.assertTrue(FileSystem.ls(Paths.get(directory)).count() == 0);
        String str = FileOps
                .read(Resources.getAbsolutePath("/long-string.txt"));
        TObject value = Convert.javaToThrift(Tag.create(str));
        int expected = 0;
        while (segment.table().length() <= Integer.MAX_VALUE) {
            System.out.println(segment.table().length());
            segment.acquire(Write.add(Random.getSimpleString(), value, 1));
            ++expected;
        }
        System.out
                .println(segment.table().length() + " vs " + Integer.MAX_VALUE);
        Path file = Paths.get(directory).resolve(UUID.randomUUID().toString());
        segment.transfer(file);
        System.out.println(file);
        Assert.assertTrue(FileSystem.ls(Paths.get(directory)).count() > 0);
        segment = Segment.load(file);
        long actual = segment.writes().count();
        System.out.println("Expected " + expected
                + " revisions and there are actually " + actual);
        Assert.assertEquals(expected, actual);
    }

}
