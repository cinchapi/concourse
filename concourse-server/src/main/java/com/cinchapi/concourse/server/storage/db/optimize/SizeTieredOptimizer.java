/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.db.optimize;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

import com.cinchapi.concourse.server.storage.db.disk.Segment;
import com.google.common.collect.Streams;

/**
 *
 *
 * @author jeff
 */
public class SizeTieredOptimizer extends Optimizer {

    /**
     * Construct a new instance.
     * 
     * @param environment
     * @param segments
     * @param garbage
     * @param lock
     */
    protected SizeTieredOptimizer(String environment, List<Segment> segments,
            List<Segment> garbage, Lock lock, Supplier<Path> fileProvider) {
        super(environment, segments, garbage, lock, fileProvider);
    }

    @Override
    public Segment merge(Segment a, Segment b) {
        if(a.similarityWith(b) > 50) { // TODO: make configurable
            Segment merged = Segment.create();
            Streams.concat(a.writes(), b.writes()).parallel()
                    .forEach(write -> merged.transfer(write));
            return merged;
        }
        else {
            return null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.cinchapi.concourse.server.storage.db.optimize.Optimizer#
     * isOptimizationPossible(com.cinchapi.concourse.server.storage.db.optimize.
     * StorageContext, com.cinchapi.concourse.server.storage.db.disk.Segment[])
     */
    @Override
    protected boolean isOptimizationPossible(StorageContext context,
            Segment... segments) {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.cinchapi.concourse.server.storage.db.optimize.Optimizer#isTriggered(
     * com.cinchapi.concourse.server.storage.db.optimize.StorageContext)
     */
    @Override
    protected boolean isTriggered(StorageContext context) {
        // TODO Auto-generated method stub
        return false;
    }

}
