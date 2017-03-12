/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.storage.db;

import static com.cinchapi.concourse.server.GlobalState.STOPWORDS;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.util.TStrings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

/**
 * A collection of n-gram indexes that enable fulltext infix searching. For
 * every word in a {@link Value}, each substring index is mapped to a
 * {@link Position}. The entire SearchIndex contains a collection of these
 * mappings.
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
@ThreadSafe
final class SearchRecord extends Record<Text, Text, Position> {

    /**
     * DO NOT INVOKE. Use {@link Record#createSearchRecord(Text)} or
     * {@link Record#createSearchRecordPartial(Text, Text)} instead.
     * 
     * @param locator
     * @param key
     */
    @PackagePrivate
    @DoNotInvoke
    SearchRecord(Text locator, @Nullable Text key) {
        super(locator, key);
    }

    /**
     * Return the Set of primary keys for records that match {@code query}.
     * 
     * @param query
     * @return the Set of PrimaryKeys
     */
    public Set<PrimaryKey> search(Text query) {
        read.lock();
        try {
            Multimap<PrimaryKey, Integer> reference = HashMultimap.create();
            String[] toks = query
                    .toString()
                    .toLowerCase()
                    .split(TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
            boolean initial = true;
            int offset = 0;
            for (String tok : toks) {
                Multimap<PrimaryKey, Integer> temp = HashMultimap.create();
                if(STOPWORDS.contains(tok)) {
                    // When skipping a stop word, we must record an offset to
                    // correctly determine if the next term match is in the
                    // correct relative position to the previous term match
                    ++offset;
                    continue;
                }
                Set<Position> positions = get(Text.wrap(tok));
                for (Position position : positions) {
                    PrimaryKey key = position.getPrimaryKey();
                    int pos = position.getIndex();
                    if(initial) {
                        temp.put(key, pos);
                    }
                    else {
                        for (int current : reference.get(key)) {
                            if(pos == current + 1 + offset) {
                                temp.put(key, pos);
                            }
                        }
                    }
                }
                initial = false;
                reference = temp;
                offset = 0;
            }

            // Result Scoring: Scoring is simply the number of times the query
            // appears in a document [e.g. the number of Positions mapped from
            // key: #reference.get(key).size()]. The total number of positions
            // in #reference is equal to the total number of times a document
            // appears in the corpus [e.g. reference.asMap().values().size()].
            Multimap<Integer, PrimaryKey> sorted = TreeMultimap.create(
                    Collections.<Integer> reverseOrder(),
                    PrimaryKey.Sorter.INSTANCE);
            for (Entry<PrimaryKey, Collection<Integer>> entry : reference
                    .asMap().entrySet()) {
                sorted.put(entry.getValue().size(), entry.getKey());
            }
            return Sets.newLinkedHashSet(sorted.values());
        }
        finally {
            read.unlock();
        }
    }

    @Override
    protected Map<Text, Set<Position>> mapType() {
        return Maps.newHashMap();
    }
}
