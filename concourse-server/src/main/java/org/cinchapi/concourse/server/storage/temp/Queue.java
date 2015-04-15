/*
 * Copyright (c) 2013-2015 Cinchapi, Inc.
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
package org.cinchapi.concourse.server.storage.temp;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.cinchapi.concourse.server.model.Value;
import org.cinchapi.concourse.server.storage.PermanentStore;
import org.cinchapi.concourse.server.storage.cache.BloomFilter;
import org.cinchapi.concourse.thrift.Type;
import org.cinchapi.concourse.util.Producer;

import com.google.common.collect.Lists;

/**
 * A {@link Queue} is a very simple form of {@link Limbo} that represents
 * data as a sequence of {@link Write} objects. New data is appended to the
 * sequence and the returned {@link Iterator} traverses the list.
 * 
 * @author Jeff Nelson
 */
public class Queue extends Limbo {

    /**
     * The threshold at which an internal bloom filter is dynamically created to
     * speed up verifies.
     */
    private static final int BLOOM_FILTER_CREATION_THRESHOLD = 10;

    /**
     * An empty array of writes that is used to specify type conversion in the
     * {@link ArrayList#toArray(Object[])} method.
     */
    private static final Write[] EMPTY_WRITES_ARRAY = new Write[0];

    /**
     * A global producer that provides BloomFilters to instances that need them.
     * To some extent, this producer will queue up bloom filters so that the
     * overhead of creating them is not incurred directly by the caller.
     */
    private static final Producer<BloomFilter> producer = new Producer<BloomFilter>(
            new Callable<BloomFilter>() {

                @Override
                public BloomFilter call() throws Exception {
                    return BloomFilter.create(500000); // TODO at some point
                                                       // this size should be
                                                       // determine based on
                                                       // some intelligent
                                                       // heuristic
                }

            });

    /**
     * Revisions are stored as a sequential list of {@link Write} objects, which
     * means most reads are <em>at least</em> an O(n) scan.
     */
    protected final List<Write> writes;

    /**
     * The bloom filter used to speed up verifies.
     */
    private BloomFilter filter = null;

    /**
     * Construct a Limbo with enough capacity for {@code initialSize}. If
     * necessary, the structure will grow to accommodate more data.
     * 
     * @param initialSize
     */
    public Queue(int initialSize) {
        writes = Lists.newArrayListWithCapacity(initialSize);
    }

    /**
     * Return an unmodifiable copy of the writes contained in the Queue.
     * 
     * @return the writes
     */
    public List<Write> getWrites() {
        return writes;
    }

    @Override
    public boolean insert(Write write, boolean sync) {
        writes.add(write); // #sync is meaningless since Queue is a memory store
        if(filter != null) {
            filter.putCached(write.getKey(), write.getValue(),
                    write.getRecord());
        }
        else if(writes.size() > BLOOM_FILTER_CREATION_THRESHOLD) {
            filter = producer.consume();
            for (int i = 0; i < writes.size(); ++i) {
                Write stored = writes.get(i);
                filter.put(stored.getKey(), stored.getValue(),
                        stored.getRecord());
            }
        }
        return true;
    }

    @Override
    public Iterator<Write> iterator() {
        return writes.iterator();
    }

    /**
     * Return the number of writes in this Queue.
     * 
     * @return the number of writes
     */
    public int size() {
        return writes.size();
    }

    @Override
    public void start() {
        // do nothing
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public void transport(PermanentStore destination, boolean sync) {
        // For transactions, this method will only be called once, so we can
        // optimize it by not using the services of an Iterator (e.g. hasNext(),
        // remove(), etc) and, if the number of writes in the Queue is large
        // enough, grabbing elements from the backing array directly.
        int length = writes.size();
        Write[] elts = length > 10000 ? writes.toArray(EMPTY_WRITES_ARRAY)
                : null;
        for (int i = 0; i < length; ++i) {
            Write write = elts == null ? writes.get(i) : elts[i];
            destination.accept(write, sync);
        }
    }

    @Override
    public boolean verify(Write write, long timestamp, boolean exists) {
        if(filter == null
                || (filter != null && filter.mightContainCached(write.getKey(),
                        write.getValue(), write.getRecord()))) {
            return super.verify(write, timestamp, exists);
        }
        else {
            return exists;
        }
    }

    @Override
    protected long getOldestWriteTimstamp() {
        // When there is no data in the buffer return the max possible timestamp
        // so that no query's timestamp is less than this timestamp
        if(writes.size() == 0) {
            return Long.MAX_VALUE;
        }
        else {
            Write oldestWrite = writes.get(0);
            return oldestWrite.getVersion();
        }

    }

    @Override
    protected Iterator<Write> getSearchIterator(String key) {
        return iterator();
    }

    @Override
    protected boolean isPossibleSearchMatch(String key, Write write, Value value) {
        return write.getKey().toString().equals(key)
                && value.getType() == Type.STRING;
    }

}
