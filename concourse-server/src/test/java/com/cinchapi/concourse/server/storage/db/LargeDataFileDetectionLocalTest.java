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
package com.cinchapi.concourse.server.storage.db;

import java.nio.file.Paths;

import org.junit.Assert;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.time.Time;
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

    public static void main(String... args) {
        String directory = FileOps.tempDir("test");
        String id = Long.toString(Time.now());
        PrimaryBlock table = PrimaryBlock.createPrimaryBlock(id, directory);
        Assert.assertTrue(FileSystem.ls(Paths.get(directory)).count() == 0);
        String str = FileOps
                .read(Resources.getAbsolutePath("/long-string.txt"));
        while (table.size() <= Integer.MAX_VALUE) {
            System.out.println(table.size());
            table.insert(PrimaryKey.wrap(Time.now()),
                    Text.wrap(Random.getSimpleString()),
                    Value.wrap(Convert.javaToThrift(str)), Time.now(),
                    Action.ADD);
            if(table.size() < 0) {
                // This means that the size has exceeded the max int value
                break;
            }
        }
        System.out.println(table.size() + " vs " + Integer.MAX_VALUE);
        table.sync();
        Assert.assertTrue(FileSystem.ls(Paths.get(directory)).count() > 0);
    }

}
