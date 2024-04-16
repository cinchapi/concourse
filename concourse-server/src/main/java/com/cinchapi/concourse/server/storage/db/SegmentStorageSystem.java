/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;

import com.cinchapi.concourse.server.storage.db.kernel.Segment;

/**
 * Provides context about the underlying filesystem, memory and state used by
 * {@link Segment} storage.
 *
 * @author Jeff Nelson
 */
public interface SegmentStorageSystem {

    /**
     * Return the total disk space available in bytes.
     * 
     * @return available disk space
     */
    public long availableDiskSpace();

    /**
     * Return a {@link Lock} that controls concurrent access to the
     * {@link #segments()}.
     * 
     * @return a {@link Lock} over the {@link #segments()}
     */
    public Lock lock();

    /**
     * Save {@code segment} to this {@link SegmentStorageSystem}.
     * 
     * @param segment
     * @return the {@link Path} where the {@link Segment} is
     *         {@link Segment#transfer(Path) transferred}
     */
    public Path save(Segment segment);

    /**
     * Return a "live" view of the {@link Segment Segments} within this
     * {@link SegmentStorageSystem}.
     * 
     * @return the contained {@link Segment segments}
     */
    public List<Segment> segments();

    /**
     * Return the total disk space in bytes.
     * 
     * @return total disk space
     */
    public long totalDiskSpace();

}
