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
package com.cinchapi.concourse.server.storage.db;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.google.common.collect.Maps;

/**
 * A collection of n-gram indexes that enable fulltext infix searching. For
 * every word in a {@link Value}, each substring index is mapped to a
 * {@link Position}. The entire SearchIndex contains a collection of these
 * mappings.
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
public final class CorpusRecord extends Record<Text, Text, Position> {

    /**
     * Return a {@link CorpusRecord} that holds data for {@code locator}.
     * 
     * @param locator
     * @return the {@link CorpusRecord}
     */
    public static CorpusRecord create(Text locator) {
        return new CorpusRecord(locator, null);
    }

    /**
     * Return a {@link CorpusRecord} that holds data for {@code key} in
     * {@code locator}.
     * 
     * @param locator
     * @param key
     * @return the {@link CorpusRecord}
     */
    public static CorpusRecord createPartial(Text locator, Text key) {
        return new CorpusRecord(locator, key);
    }

    /**
     * DO NOT INVOKE. Use {@link Record#createSearchRecord(Text)} or
     * {@link Record#createSearchRecordPartial(Text, Text)} instead.
     * 
     * @param locator
     * @param key
     */
    @PackagePrivate
    @DoNotInvoke
    CorpusRecord(Text locator, @Nullable Text key) {
        super(locator, key);
    }

    /**
     * Return every {@link Position} where the infix is located.
     * 
     * @param query
     * @return the Set of {@link Position Positions}
     */
    public Set<Position> locate(Text infix) {
        return get(infix);
    }

    /**
     * Return every {@link Position} where the infix was located at
     * {@code version}.
     * 
     * @param query
     * @param verion
     * @return the Set of {@link Position Positions}
     */
    public Set<Position> locate(Text infix, long version) {
        return get(infix, version);
    }

    @Override
    protected void checkIsOffsetRevision(
            Revision<Text, Text, Position> revision) { /* no-op */
        // NOTE: The check is ignored for a CorpusRecord instance
        // because it will legitimately appear that "duplicate" data has
        // been added if similar data is added to the same key in a record
        // at different times (i.e. adding John Doe and Johnny Doe to the
        // "name")
    }

    @Override
    protected Map<Text, Set<Position>> mapType() {
        return Maps.newHashMap();
    }

}
