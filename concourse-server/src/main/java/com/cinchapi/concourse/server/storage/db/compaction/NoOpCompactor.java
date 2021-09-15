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

import java.util.List;

import com.cinchapi.concourse.server.storage.db.SegmentStorageSystem;
import com.cinchapi.concourse.server.storage.db.kernel.Segment;

/**
 * A {@link Compactor} that doesn't do anything.
 *
 * @author Jeff Nelson
 */
public final class NoOpCompactor extends Compactor {

    /**
     * Singleton.
     */
    private static NoOpCompactor INSTANCE;

    /**
     * Return a {@link NoOpCompactor}
     * 
     * @return {@link NoOpCompactor}
     */
    public static NoOpCompactor instance() {
        return INSTANCE;
    }

    /**
     * Construct a new instance.
     * 
     * @param storage
     */
    private NoOpCompactor(SegmentStorageSystem storage) {
        super(storage);
    }

    @Override
    protected List<Segment> compact(Segment... segments) {
        return null;
    }

}
