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
package com.cinchapi.concourse.server.storage.db.search;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.Action;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link SearchIndexer}
 * {@link SearchIndex#index(Text, Text, Position, long, Action) inserts} terms
 * and associated metadata into a {@link SearchIndex} to facilitate full-text
 * searching.
 * <p>
 * The {@link SearchIndexer} works asynchronously. A request for index insertion
 * happens by
 * {@link #enqueue(SearchIndex, CountUpLatch, Text, String, Position, long, Action)
 * enqueing} the term and metadata. The {@link SearchIndexer} will process
 * insertion requests and signal completion by incrementing a
 * {@link CountUpLatch} that is associated with each
 * {@link #enqueue(SearchIndex, CountUpLatch, Text, String, Position, long, Action)
 * request}. The caller can use the {@link CountUpLatch} to
 * {@link CountUpLatch#await(int)} for all the requested insertions to complete.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class SearchIndexer {

    /**
     * Return a {@link SearchIndexer} with {@code numWorkers} worker threads.
     * 
     * @param numWorkers
     * @return the {@link SearchIndexer}
     */
    public static SearchIndexer create(int numWorkers) {
        return new SearchIndexer(numWorkers);
    }

    /**
     * The queue where insertion requests are held.
     */
    private final BlockingQueue<Entry> queue;

    /**
     * An {@link ExecutorService} that controls the worker threads.
     */
    private final ExecutorService workers;

    /**
     * Construct a new instance.
     * 
     * @param numWorkers
     */
    private SearchIndexer(int numWorkers) {
        this.queue = Queues.newLinkedBlockingQueue();
        this.workers = Executors.newFixedThreadPool(numWorkers,
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("Search Indexer" + " %d").build());
        for (int i = 0; i < numWorkers; ++i) {
            workers.execute(() -> {
                for (;;) {
                    // Each worker repeatedly grabs an Entry from the #queue
                    // (blocking if necessary) and inserts the appropriate data
                    // into the Entry's SearchIndex.
                    try {
                        Entry entry = queue.take();
                        SearchIndex index = entry.index;
                        index.index(entry.key, Text.wrapCached(entry.term),
                                entry.position, entry.version, entry.type);
                        entry.ticker.countUp();
                    }
                    catch (InterruptedException e) {
                        throw CheckedExceptions.wrapAsRuntimeException(e);
                    }
                }
            });
        }
    }

    /**
     * Schedule an insertion into the search {@code index} for {@code term} and
     * {@code position} for {@code key}. When the insertion is complete, the
     * {@code ticker} will be {@link CountUpLatch#countUp() incremented} by 1.
     * 
     * @param index
     * @param ticker
     * @param key
     * @param term
     * @param position
     * @param version
     * @param type
     */
    public void enqueue(SearchIndex index, CountUpLatch ticker, Text key,
            String term, Position position, long version, Action type) {
        Entry entry = new Entry(index, ticker, key, term, position, version,
                type);
        queue.add(entry);
    }

    /**
     * Entry held in the {@link #queue}.
     *
     * @author Jeff Nelson
     */
    @Immutable
    private class Entry {

        /**
         * The key (e.g. the corresponding SearchBlock's locator) for which the
         * {@link #term} at {@link #position} is searchable.
         */
        private final Text key;

        /**
         * The search term.
         */
        private final String term;

        /**
         * The {@link Position} of the search term in the data universe.
         */
        private final Position position;

        /**
         * The version of the data that produced this {@link Entry}.
         */
        private final long version;

        /**
         * The {@link Action} that produced this data.
         */
        private final Action type;

        /**
         * A {@link CountUpLatch} that is incremented when this {@link Entry} is
         * {@link SearchIndex#index(Text, Text, Position, long, Action)
         * indexed}.
         */
        private final CountUpLatch ticker;

        /**
         * The {@link SearchIndex} where this {@link Entry} will be
         * {@link SearchIndex#index(Text, Text, Position, long, Action) indexed}
         * and stored.
         */
        private final SearchIndex index;

        /**
         * Construct a new instance.
         * 
         * @param index
         * @param ticker
         * @param key
         * @param term
         * @param position
         * @param version
         * @param type
         */
        Entry(SearchIndex index, CountUpLatch ticker, Text key, String term,
                Position position, long version, Action type) {
            this.index = index;
            this.ticker = ticker;
            this.key = key;
            this.term = term;
            this.position = position;
            this.version = version;
            this.type = type;
        }
    }

}
