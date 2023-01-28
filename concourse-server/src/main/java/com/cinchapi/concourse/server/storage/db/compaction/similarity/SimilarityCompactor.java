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
package com.cinchapi.concourse.server.storage.db.compaction.similarity;

import java.util.List;

import com.cinchapi.concourse.server.storage.db.SegmentStorageSystem;
import com.cinchapi.concourse.server.storage.db.compaction.Compactor;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

/**
 * A {@link Compactor} that merges {@link Segment Segments} with
 * {@link Segment#similarityWith(Segment) similar} data.
 *
 * @author Jeff Nelson
 */
public class SimilarityCompactor extends Compactor {

    /**
     * The minimum {@link Segment#similarityWith(Segment) similarity} between
     * two Segments that must be met in order for them to be merged.
     */
    // TODO: make configurable
    private double minimumSimilarityThreshold = 0.5;

    /**
     * Construct a new instance.
     * 
     * @param storage
     */
    public SimilarityCompactor(SegmentStorageSystem storage) {
        super(storage);
    }

    @Override
    protected List<Segment> compact(Segment... segments) {
        if(segments.length == 2) {
            Segment a = segments[0];
            Segment b = segments[1];
            long requiredDiskSpace = a.length() + b.length();
            if(storage().availableDiskSpace() > requiredDiskSpace
                    && a.similarityWith(b) > minimumSimilarityThreshold) {
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

    /**
     * Set the {@link #minimumSimilarityThreshold}.
     * 
     * @param minimumSimilarityThreshold
     */
    @VisibleForTesting
    protected void minimumSimilarityThreshold(
            double minimumSimilarityThreshold) {
        this.minimumSimilarityThreshold = minimumSimilarityThreshold;
    }

}
