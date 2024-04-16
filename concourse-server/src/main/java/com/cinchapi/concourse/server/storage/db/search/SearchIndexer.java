/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cinchapi.common.concurrent.CountUpLatch;
import com.cinchapi.concourse.server.model.Position;
import com.cinchapi.concourse.server.model.Text;
import com.cinchapi.concourse.server.storage.Action;
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
     * An {@link ExecutorService} that controls the worker threads.
     */
    private final ExecutorService workers;

    /**
     * Construct a new instance.
     * 
     * @param numWorkers
     */
    private SearchIndexer(int numWorkers) {
        this.workers = Executors.newFixedThreadPool(numWorkers,
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("Search Indexer" + " %d").build());
    }

    /**
     * Schedule an insertion into the search {@code index} for {@code term} and
     * {@code position} for {@code key}. When the insertion is complete, the
     * {@code tracker} will be {@link CountUpLatch#countUp() incremented} by 1.
     * 
     * @param index
     * @param tracker
     * @param key
     * @param term
     * @param position
     * @param version
     * @param type
     * @param artifacts
     */
    public <T> void enqueue(SearchIndex index, CountUpLatch tracker, Text key,
            Text term, Position position, long version, Action type,
            Collection<T> artifacts) {
        workers.execute(() -> {
            index.index(key, term, position, version, type, artifacts);
            tracker.countUp();
        });
    }

}
