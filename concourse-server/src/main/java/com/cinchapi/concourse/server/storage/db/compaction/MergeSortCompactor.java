/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

/**
 * A {@link Compactor} that merges {@link Segment Segments} with
 * {@link Segment#similarityWith(Segment) similar} data.
 *
 * @author Jeff Nelson
 */
class MergeSortCompactor extends Compactor {

    /**
     * Construct a new instance.
     * 
     * @param environment
     * @param segments
     * @param garbage
     * @param lock
     * @param fileProvider
     * @param minorInitialDelayInSeconds
     * @param minorRunFrequencyInSeconds
     * @param majorInitialDelayInSeconds
     * @param majorRunFrequencyInSeconds
     */
    protected MergeSortCompactor(String environment, List<Segment> segments,
            List<Segment> garbage, Lock lock, Supplier<Path> fileProvider,
            long minorInitialDelayInSeconds, long minorRunFrequencyInSeconds,
            long majorInitialDelayInSeconds, long majorRunFrequencyInSeconds) {
        super(environment, segments, garbage, lock, fileProvider,
                minorInitialDelayInSeconds, minorRunFrequencyInSeconds,
                majorInitialDelayInSeconds, majorRunFrequencyInSeconds);
    }

    @Override
    protected List<Segment> compact(StorageContext context,
            Segment... segments) {
        if(segments.length == 2) {
            Segment a = segments[0];
            Segment b = segments[1];
            long requiredDiskSpace = a.length() + b.length();
            if(context.availableDiskSpace() > requiredDiskSpace
                    && a.similarityWith(b) > 50) { // TODO: make configurable
                Segment merged = Segment.create((int) (a.count() + b.count())); // TODO:
                                                                                // create
                                                                                // offheap
                Streams.concat(a.writes(), b.writes()).parallel()
                        .forEach(write -> merged.acquire(write));
                return ImmutableList.of(merged);
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

}
