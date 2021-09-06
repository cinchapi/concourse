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
package com.cinchapi.concourse.server.storage.db.kernel;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.common.util.PossibleCloseables;
import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.Composite;
import com.cinchapi.concourse.server.model.Identifier;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.db.CorpusRevision;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.server.storage.db.search.OffHeapTextSet;
import com.cinchapi.concourse.server.storage.db.search.SearchIndex;
import com.cinchapi.concourse.server.storage.db.search.SearchIndexer;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.TStrings;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * A {@link Chunk} that stores {@link CorpusRevision CorpusRevisons} for
 * various {@link CorpusRecord CorpusRecords}.
 *
 * @author Jeff Nelson
 */
public class CorpusChunk extends ConcurrentChunk<Text, Text, Position>
        implements
        SearchIndex {

    /**
     * Global flag that indicates if artifacts should be recorded when
     * {@link #index(Text, Text, Position, long, Action, Collection) indexing}.
     */
    private final static boolean TRACK_ARTIFACTS = GlobalState.ENABLE_SEARCH_CACHE;

    /**
     * {@link GlobalState#STOPWORDS} mapped to {@link Text} so that they can be
     * used in the
     * {@link #prepare(CountUpLatch, Text, String, Identifier, int, long, Action, Collection)}
     * method.
     */
    private final static Set<Text> STOPWORDS = GlobalState.STOPWORDS.stream()
            .map(Text::wrap)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    /**
     * Return a new {@link CorpusChunk}.
     * 
     * @param filter
     * @return the created {@link Chunk}
     */
    public static CorpusChunk create(BloomFilter filter) {
        return create(null, filter);
    }

    /**
     * Return a new {@link CorpusChunk}.
     * 
     * @param segment
     * @param filter
     * @return the created {@link Chunk}
     */
    public static CorpusChunk create(@Nullable Segment segment,
            BloomFilter filter) {
        return new CorpusChunk(segment, filter);
    }

    /**
     * Load an existing {@link CorpusChunk}.
     * 
     * @param file
     * @param position
     * @param size
     * @param filter
     * @param manifest
     * @return the loaded {@link Chunk}
     */
    public static CorpusChunk load(Path file, long position, long size,
            BloomFilter filter, Manifest manifest) {
        return new CorpusChunk(null, file, null, position, size, filter,
                manifest);
    }

    /**
     * Load an existing {@link CorpusChunk}.
     * 
     * @param segment
     * @param position
     * @param size
     * @param filter
     * @param manifest
     * @return the loaded {@link Chunk}
     */
    public static CorpusChunk load(@Nonnull Segment segment, long position,
            long size, BloomFilter filter, Manifest manifest) {
        return new CorpusChunk(segment, segment.file(), segment.channel(),
                position, size, filter, manifest);
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
     * The service is static (and therefore shared by each {@link CorpusChunk})
     * because
     * only one segment at a time should be mutable and able to process inserts.
     * </p>
     * <p>
     * If multiple environments are active, they can all use this shared INDEXER
     * without blocking.
     * </p>
     */
    private static final SearchIndexer INDEXER = SearchIndexer
            .create(NUM_INDEXER_THREADS);

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param filter
     */
    private CorpusChunk(@Nullable Segment segment, BloomFilter filter) {
        super(segment, filter);
    }

    /**
     * Construct a new instance.
     * 
     * @param segment
     * @param file
     * @param channel
     * @param position
     * @param size
     * @param filter
     * @param manifest
     */
    private CorpusChunk(@Nullable Segment segment, Path file,
            FileChannel channel, long position, long size, BloomFilter filter,
            Manifest manifest) {
        super(segment, file, channel, position, size, filter, manifest);
    }

    /**
     * DO NOT CALL DIRECTLY.
     * <p>
     * {@inheritDoc}
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> void index(Text key, Text term, Position position, long version,
            Action type, Collection<T> artifacts) {
        Artifact<Text, Text, Position> artifact = (Artifact<Text, Text, Position>) super.insertUnsafe(
                key, term, position, version, type);
        artifacts.add((T) artifact);
    }

    /**
     * DO NOT CALL. Use {@link #insert(Text, Value, Identifier)} instead.
     */
    @Override
    @DoNotInvoke
    public Artifact<Text, Text, Position> insert(Text locator, Text key,
            Position value, long version, Action type)
            throws IllegalStateException {
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
    public final Collection<CorpusArtifact> insert(Text key, Value value,
            Identifier record, long version, Action type) {
        Preconditions.checkState(isMutable(),
                "Cannot modify a chunk that is immutable");
        if(value.getType() == Type.STRING) {
            write.lock();
            try {
                String string = value.getObject().toString().toLowerCase(); // CON-10
                String[] toks = string.split(
                        TStrings.REGEX_GROUP_OF_ONE_OR_MORE_WHITESPACE_CHARS);
                Collection<CorpusArtifact> artifacts = TRACK_ARTIFACTS
                        ? new ConcurrentLinkedQueue<>()
                        : new NoOpList<>();
                CountUpLatch tracker = new CountUpLatch();
                int pos = 0;
                int numPrepared = 0;
                for (String tok : toks) {
                    numPrepared += prepare(tracker, key, tok, record, pos,
                            version, type, artifacts);
                    ++pos;
                }
                try {
                    tracker.await(numPrepared);
                }
                catch (InterruptedException e) {
                    throw CheckedExceptions.wrapAsRuntimeException(e);
                }
                return artifacts;
            }
            finally {
                write.unlock();
            }
        }
        else {
            return ImmutableList.of();
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected SortedSet<Revision<Text, Text, Position>> createBackingStore(
            Comparator<Revision> comparator) {
        return new ConcurrentSkipListSet<>(comparator);
    }

    @Override
    protected CorpusArtifact makeArtifact(
            Revision<Text, Text, Position> revision, Composite[] composites) {
        // If artifact tracking is disabled, don't unnecessarily hold a
        // reference to the composites in hopes that memory can GCed
        composites = TRACK_ARTIFACTS ? composites : Array.containing();
        return new CorpusArtifact((CorpusRevision) revision, composites);
    }

    @Override
    protected CorpusRevision makeRevision(Text locator, Text key,
            Position value, long version, Action type) {
        return Revision.createCorpusRevision(locator, key, value, version,
                type);
    }

    @Override
    protected Class<CorpusRevision> xRevisionClass() {
        return CorpusRevision.class;
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
     * @param artifacts a collection where each {@link Chunk.Artifact} that is
     *            generated from the
     *            {@link #index(Text, Text, Position, long, Action, Collection)}
     *            job is stored
     * @return the number of inserts that have been enqueued so that the caller
     *         can {@link CountUpLatch#await(int) await} all related inserts
     *         to finish.
     */
    private int prepare(CountUpLatch tracker, Text key, String term,
            Identifier record, int position, long version, Action type,
            Collection<CorpusArtifact> artifacts) {
        int count = 0;
        if(!GlobalState.STOPWORDS.contains(term)) {
            Position pos = Position.of(record, position);
            int length = term.length();
            int upperBound = length * (length + 1) / 2; // https://www.geeksforgeeks.org/number-substrings-string/

            // Detect if the #term is large enough to likely cause OOMs when
            // indexing and prepare the appropriate precautions.
            boolean isLargeTerm = upperBound > 5000000;

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
            Set<Text> indexed = isLargeTerm 
                    ? OffHeapTextSet.create(upperBound)
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
                    // @formatter:off
                    Text infix = (isLargeTerm 
                            ? Text.wrap(chars, i, j)
                            : Text.wrapCached(term.substring(i, j))).trim();
                    // @formatter:on
                    if(!infix.isEmpty() && !STOPWORDS.contains(infix)
                            && indexed.add(infix)) {
                        INDEXER.enqueue(this, tracker, key, infix, pos, version,
                                type, artifacts);
                        ++count;
                    }
                }
            }
            PossibleCloseables.tryCloseQuietly(indexed);
            indexed = null; // make eligible for immediate GC
        }
        return count;
    }

    /**
     * A {@link List} that ignores attempts to write any data.
     *
     * @author Jeff Nelson
     */
    private static class NoOpList<T> extends AbstractList<T> {

        @Override
        public T get(int index) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean add(T e) {
            return false;
        }

    }

}
