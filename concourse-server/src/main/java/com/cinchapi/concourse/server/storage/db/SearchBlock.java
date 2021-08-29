/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import static com.cinchapi.concourse.server.GlobalState.STOPWORDS;

import java.util.Comparator;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.internal.announcer.InternalAnnouncer;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.common.util.PossibleCloseables;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.PrimaryKey;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.db.search.SearchIndex;
import com.cinchapi.concourse.server.storage.db.search.SearchIndexer;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.ConcurrentSkipListMultiset;
import com.cinchapi.concourse.util.Logging;
import com.cinchapi.concourse.util.TStrings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedMultiset;

/**
 * A Block that stores SearchRevision data to be used in a SearchRecord.
 * <p>
 * Text is indexed in a block such that that a value matches a query if it
 * contains a sequence of terms where each term or a substring of that term
 * matches the term in the same relative position of the query (i.e. if the
 * query is for 'fo ar' then value 'foo bar' will match, etc).
 * </p>
 * <p>
 * </p>
 * 
 * @author Jeff Nelson
 */
@ThreadSafe
@PackagePrivate
final class SearchBlock extends Block<Text, Text, Position> implements
        SearchIndex {

    static {
        Logging.disable(Jvm.class);
        Logging.disable(InternalAnnouncer.class);
        Logging.disable(VanillaChronicleMap.class);
    }

    /**
     * The number of worker threads to reserve for the {@link SearchIndexer}.
     */
    private static int NUM_INDEXER_THREADS = Math.max(3,
            (int) Math.round(0.5 * Runtime.getRuntime().availableProcessors()));

    /**
     * The {@link SearchIndexer} that is responsible for multithreaded search
     * indexing.
     * <p>
     * The service is static (and therefore shared by each SearchBlock) because
     * only one search block at a time should be mutable and able to process
     * inserts.
     * </p>
     * <p>
     * If multiple environments are active, they can all use this shared INDEXER
     * without blocking.
     * </p>
     */
    private static final SearchIndexer INDEXER = SearchIndexer
            .create(NUM_INDEXER_THREADS);

    /**
     * DO NOT CALL!!
     * 
     * @param id
     * @param directory
     * @param diskLoad
     */
    @PackagePrivate
    @DoNotInvoke
    SearchBlock(String id, String directory, boolean diskLoad) {
        super(id, directory, diskLoad);
        this.concurrent = true;
    }

    /**
     * DO NOT CALL DIRECTLY.
     * <p>
     * {@inheritDoc}
     * </p>
     */
    @Override
    public void index(Text key, Text term, Position position, long version,
            Action type) {
        super.insertUnsafe(key, term, position, version, type);
    }

    /**
     * DO NOT CALL. Use {@link #insert(Text, Value, PrimaryKey)} instead.
     */
    @Override
    @DoNotInvoke
    public final SearchRevision insert(Text locator, Text key, Position value,
            long version, Action type) {
        throw new UnsupportedOperationException();
    }

    /**
     * Insert a revision for {@code key} as {@code value} in {@code record} at
     * {@code version}
     * 
     * @param key
     * @param value
     * @param record
     * @param version
     * @param type
     */
    public final void insert(Text key, Value value, PrimaryKey record,
            long version, Action type) {
        Preconditions.checkState(mutable,
                "Cannot modify a block that is not mutable");
        if(value.getType() == Type.STRING) {
            String string = value.getObject().toString().toLowerCase(); // CON-10
            String[] toks = string.split(
                    TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
            CountUpLatch tracker = new CountUpLatch();
            int pos = 0;
            int numPrepared = 0;
            for (String tok : toks) {
                numPrepared += prepare(tracker, key, tok, record, pos, version,
                        type);
                ++pos;
            }
            try {
                tracker.await(numPrepared);
            }
            catch (InterruptedException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
    }

    /**
     * Calculate all possible substrings for {@code term} and
     * {@link SearchIndexer#enqueue(SearchIndex, CountUpLatch, Text, String, Position, long, Action)
     * enqueue} work that will store a revision for the {@code term} at
     * {@code position} for {@code key} in {@code record} at {@code version}.
     * 
     * @param tracker a {@link CountUpLatch} that is associated with each of the
     *            tasks that are
     *            {@link SearchIndexer#enqueue(SearchIndex, CountUpLatch, Text, String, Position, long, Action)
     *            enqueued} by this method; when each index task completes, it
     *            {@link CountUpLatch#countUp() increments} the tracker
     * @param key
     * @param term
     * @param record
     * @param position
     * @param version
     * @param type
     * @return the number of inserts that have been enqueued so that the caller
     *         can {@link CountUpLatch#await(int) await} all related inserts
     *         to finish.
     */
    private int prepare(CountUpLatch tracker, Text key, String term,
            PrimaryKey record, int position, long version, Action type) {
        int count = 0;
        if(!STOPWORDS.contains(term)) {
            Position pos = Position.wrap(record, position);
            int length = term.length();
            int upperBound = (int) Math.pow(length, 2);

            // Detect if the #term is large enough to likely cause OOMs when
            // indexing and prepare the appropriate precautions.
            boolean isLargeTerm = upperBound > 10000000;

            // A flag that indicates whether the {@link #prepare(CountUpLatch,
            // Text, String, PrimaryKey, int, long, Action) prepare} function
            // should limit the length of substrings that are indexed.
            // Generally, this value is {@code true} if the configuration has a
            // value for {@link GlobalState#MAX_SEARCH_SUBSTRING_LENGTH} that is
            // greater than 0.
            // NOTE: This is NOT static because unit tests sequencing would
            // cause this to fail :-/
            boolean shouldLimitSubstringLength = GlobalState.MAX_SEARCH_SUBSTRING_LENGTH > 0;

            // The set of substrings that have been indexed from {@code term} at
            // {@code position} for {@code key} in {@code record} at {@code
            // version}. This is used to ensure that we do not add duplicate
            // indexes (i.e. 'abrakadabra')
            // @formatter:off
            Set<String> indexed = isLargeTerm 
                    ? ChronicleSet.of(String.class).averageKey(
                            term.substring(0, shouldLimitSubstringLength
                                    ? GlobalState.MAX_SEARCH_SUBSTRING_LENGTH
                                    : 100))
                            .entries(upperBound).create()
                    : Sets.newHashSetWithExpectedSize(upperBound);
            // @formatter:on
            final char[] chars = isLargeTerm ? term.toCharArray() : null;
            for (int i = 0; i < length; ++i) {
                int start = i + 1;
                int limit = (shouldLimitSubstringLength
                        ? Math.min(length,
                                start + GlobalState.MAX_SEARCH_SUBSTRING_LENGTH)
                        : length) + 1;
                for (int j = start; j < limit; ++j) {
                    String substring = term.substring(i, j).trim();
                    // @formatter:off
                    Text infix = isLargeTerm 
                            ? Text.wrap(chars, i, j).trim()
                            : Text.wrapCached(substring);
                    // @formatter:on
                    if(!Strings.isNullOrEmpty(substring)
                            && !STOPWORDS.contains(substring)
                            && indexed.add(substring)) {
                        INDEXER.enqueue(this, tracker, key, infix, pos, version,
                                type);
                        ++count;
                    }
                }
            }
            PossibleCloseables.tryCloseQuietly(indexed);
            indexed = null; // make eligible for immediate GC
        }
        return count;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected SortedMultiset<Revision<Text, Text, Position>> createBackingStore(
            Comparator<Revision> comparator) {
        return ConcurrentSkipListMultiset.create(comparator);
    }

    @Override
    protected SearchRevision makeRevision(Text locator, Text key,
            Position value, long version, Action type) {
        return Revision.createSearchRevision(locator, key, value, version,
                type);
    }

    @Override
    protected Class<SearchRevision> xRevisionClass() {
        return SearchRevision.class;
    }

}
