/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.compaction;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.SegmentStorageSystem;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;

/**
 * Utils for {@link Compactor} tests.
 *
 * @author Jeff Nelson
 */
public final class CompactorTests {

    /**
     * Return a {@link SegmentStorageSystem} to use in a test.
     * 
     * @return the {@link SegmentStorageSystem}
     */
    public static SegmentStorageSystem getStorageSystem() {
        return new SegmentStorageSystem() {

            File fs = new File(FileSystem.tempFile());
            List<Segment> segments = new ArrayList<>();
            Lock lock = new ReentrantLock();

            @Override
            public long availableDiskSpace() {
                return fs.getUsableSpace();
            }

            @Override
            public Lock lock() {
                return lock;
            }

            @Override
            public Path save(Segment segment) {
                Path file = Paths.get(FileSystem.tempFile());
                segment.transfer(file);
                return file;
            }

            @Override
            public List<Segment> segments() {
                return segments;
            }

            @Override
            public long totalDiskSpace() {
                return fs.getTotalSpace();
            }

        };
    }

    private CompactorTests() {/* no-init */}

}
