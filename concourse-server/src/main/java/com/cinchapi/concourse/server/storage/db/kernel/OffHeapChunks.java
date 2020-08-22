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

import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.io.Byteables;
import com.cinchapi.concourse.server.io.OffHeapMemoryByteSink;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.lib.offheap.collect.OffHeapSortedMultiset;
import com.cinchapi.lib.offheap.io.Serializer;
import com.cinchapi.lib.offheap.memory.OffHeapMemory;
import com.cinchapi.lib.offheap.memory.UnsafeMemory;

/**
 *
 *
 * @author jeff
 */
class OffHeapChunks {

    @SuppressWarnings("rawtypes")
    public static <L extends Byteable & Comparable<L>, K extends Byteable & Comparable<K>, V extends Byteable & Comparable<V>> OffHeapSortedMultiset<Revision<L, K, V>> createOffHeapBackingStore(
            Comparator<Revision> comparator,
            Class<? extends Revision<L, K, V>> revisionClass) {
        OffHeapMemory memory = new UnsafeMemory(GlobalState.BUFFER_PAGE_SIZE);
        Serializer<Revision<L, K, V>> serializer = new Serializer<Revision<L, K, V>>() {

            @Override
            public void serialize(Revision<L, K, V> element,
                    OffHeapMemory memory) {
                element.copyTo(new OffHeapMemoryByteSink(memory));
            }

            @Override
            public Revision<L, K, V> deserialize(OffHeapMemory memory) {
                return Byteables.read(memory, revisionClass);
            }

            @Override
            public int sizeOf(Revision<L, K, V> element) {
                return element.size();
            }

        };
        OffHeapSortedMultiset<Revision<L, K, V>> store = new OffHeapSortedMultiset<Revision<L, K, V>>(
                memory, (o1, o2) -> comparator.compare(o1, o2), serializer);
        return store;
    }

}
