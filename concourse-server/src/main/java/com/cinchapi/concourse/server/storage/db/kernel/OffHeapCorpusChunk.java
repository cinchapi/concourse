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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.util.Comparator;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.google.common.collect.SortedMultiset;

/**
 *
 *
 * @author jeff
 */
@NotThreadSafe
class OffHeapCorpusChunk extends CorpusChunk {

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param filter
     */
    protected OffHeapCorpusChunk(CorpusChunk proxy) {
        super(proxy.segment(), proxy.filter());
        Reflection.set("$revisions", null, this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected SortedMultiset<Revision<Text, Text, Position>> createBackingStore(
            Comparator<Revision> comparator) {
        return OffHeapChunks.createOffHeapBackingStore(comparator,
                xRevisionClass());
    }

}
