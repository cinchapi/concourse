/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage.temp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.cinchapi.concourse.server.model.Value;
import com.cinchapi.concourse.server.storage.Action;
import com.cinchapi.concourse.server.storage.DurableStore;
import com.cinchapi.concourse.server.storage.cache.BloomFilter;
import com.cinchapi.concourse.server.storage.view.Table;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.EagerProducer;

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
     * A global producer that provides BloomFilters to instances that need them.
     * To some extent, this producer will queue up bloom filters so that the
     * overhead of creating them is not incurred directly by the caller.
     */
    private static final EagerProducer<BloomFilter> BLOOM_FILTER_PRODUCER = EagerProducer
            .of(() -> {
                // TODO: at some point this size should be determined based
                // on some intelligent heuristic
                BloomFilter filter = BloomFilter.create(500000);
                return filter;
            });

    /**
     * An empty array of writes that is used to specify type conversion in the
     * {@link ArrayList#toArray(Object[])} method.
     */
    private static final Write[] EMPTY_WRITES_ARRAY = new Write[0];

    /**
     * The bloom filter used to speed up verifies.
     */
    private BloomFilter filter = null;

    /**
     * A cache of the presently contained data that is used to speed up reads.
     * <p>
     * Since there is overhead to maintaining this data during write operations,
     * it is only utilized if the number of {@link Write Writes} in this store
     * exceeds {@link #BLOOM_FILTER_CREATION_THRESHOLD}.
     * </p>
     */
    private Table table = null;

    /**
     * A cache of the {@link #getOldestWriteTimstamp() timestamp} for the oldest
     * write in the Queue. This value is not expected to change often, so it
     * makes sense to cache it for situations that frequently look for it.
     */
    private long oldestWriteTimestampCache = 0;

    /**
     * Revisions are stored as a sequential list of {@link Write} objects, which
     * means most reads are <em>at least</em> an O(n) scan.
     */
    private final List<Write> writes;

    /**
     * Construct a Limbo with enough capacity for {@code initialSize}. If
     * necessary, the structure will grow to accommodate more data.
     * 
     * @param initialSize
     */
    public Queue(int initialSize) {
        this(new ArrayList<>(initialSize));
    }

    /**
     * Construct a new instance.
     * 
     * @param writes
     */
    protected Queue(List<Write> writes) {
        this.writes = writes;
    }

    @Override
    public Set<String> describe(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        if(context.isEmpty() && timestamp == Time.NONE && isReadOptimized()) {
            return table().select(record).keySet();
        }
        else {
            return super.describe(record, timestamp, context);
        }
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
        if(table != null) {
            table.put(write);
        }
        return true;
    }

    @Override
    public Iterator<Write> iterator() {
        return writes.iterator();
    }

    @Override
    public Map<String, Set<TObject>> select(long record, long timestamp,
            Map<String, Set<TObject>> context) {
        if(context.isEmpty() && timestamp == Time.NONE && isReadOptimized()) {
            Map<String, Set<TObject>> data = new TreeMap<>(
                    (s1, s2) -> s1.compareToIgnoreCase(s2));
            data.putAll(table().select(record));
            return data;
        }
        else {
            return super.select(record, timestamp, context);
        }
    }

    @Override
    public Set<TObject> select(String key, long record, long timestamp,
            Set<TObject> context) {
        if(context.isEmpty() && timestamp == Time.NONE && isReadOptimized()) {
            return table().lookup(record, key);
        }
        else {
            return super.select(key, record, timestamp, context);
        }
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
    public void transform(Function<Write, Write> transformer) {
        for (int i = 0; i < writes.size(); ++i) {
            Write write = writes.get(i);
            writes.set(i, transformer.apply(write));
        }
    }

    @Override
    public void transport(DurableStore destination, boolean sync) {
        // For transactions, this method will only be called once, so we can
        // optimize it by not using the services of an Iterator (e.g. hasNext(),
        // remove(), etc) and, if the number of writes in the Queue is large
        // enough, grabbing elements from the backing array directly.
        oldestWriteTimestampCache = 0;
        int length = writes.size();
        Write[] elts = length > 10000 ? writes.toArray(EMPTY_WRITES_ARRAY)
                : null;
        for (int i = 0; i < length; ++i) {
            Write write = elts == null ? writes.get(i) : elts[i];
            destination.accept(write, sync);
        }
    }

    @Override
    public boolean verify(Write write, long timestamp) {
        if(!isReadOptimized()
                || (isReadOptimized() && filter().mightContainCached(
                        write.getKey(), write.getValue(), write.getRecord()))) {
            return super.verify(write, timestamp);
        }
        else {
            return false;
        }
    }

    @Override
    @Nullable
    protected Action getLastWriteAction(Write write, long timestamp) {
        if(!isReadOptimized()
                || (isReadOptimized() && filter().mightContainCached(
                        write.getKey(), write.getValue(), write.getRecord()))) {
            return super.getLastWriteAction(write, timestamp);
        }
        else {
            return null;
        }
    }

    @Override
    protected long getOldestWriteTimestamp() {
        // When there is no data in the buffer return the max possible timestamp
        // so that no query's timestamp is less than this timestamp
        if(writes.size() == 0) {
            return Long.MAX_VALUE;
        }
        else {
            if(oldestWriteTimestampCache == 0) {
                Write oldestWrite = writes.get(0);
                oldestWriteTimestampCache = oldestWrite.getVersion();
            }
            return oldestWriteTimestampCache;
        }

    }

    @Override
    protected Iterator<Write> getSearchIterator(String key) {
        return iterator();
    }

    @Override
    protected boolean isPossibleSearchMatch(String key, Write write,
            Value value) {
        return write.getKey().toString().equals(key)
                && value.getType() == Type.STRING;
    }

    /**
     * Returns the {@link #filter} if this {@link Queue} is
     * {@link #isReadOptimized() read optimized}, creating it on demand if
     * necessary.
     * <p>
     * This method should <b>only</b> be used for read operations that rely on
     * the {@link #filter}. It ensures that the table is generated and populated
     * only when needed, preventing unnecessary memory consumption.
     * </p>
     * <p>
     * <b>Important:</b> Do not use this method for write operations. Instead,
     * access {@link #filter} directly and check for {@code null} before
     * writing.
     * </p>
     * 
     * @return the {@link #filter} if this {@link Queue} is read optimized;
     *         otherwise, returns {@code null}.
     */
    @Nullable
    private BloomFilter filter() {
        if(isReadOptimized()) {
            if(filter == null) {
                filter = BLOOM_FILTER_PRODUCER.consume();
                for (Write write : writes) {
                    filter.putCached(write.getKey(), write.getValue(),
                            write.getRecord());
                }
            }
            return filter;
        }
        else {
            return null;
        }
    }

    /**
     * Return a boolean that indicates whether this {@link Queue} optimizes
     * reads by using internal structures that attempt to reduce the need for
     * log replay.
     * 
     * @return {@code true} if this {@link Queue} is read optimized
     */
    private boolean isReadOptimized() {
        return writes.size() > BLOOM_FILTER_CREATION_THRESHOLD;
    }

    /**
     * Returns the {@link #table} if this {@link Queue} is
     * {@link #isReadOptimized() read optimized}, creating it on demand if
     * necessary.
     * <p>
     * This method should <b>only</b> be used for read operations that rely on
     * the {@link #table}. It ensures that the table is generated and populated
     * only when needed, preventing unnecessary memory consumption.
     * </p>
     * <p>
     * <b>Important:</b> Do not use this method for write operations. Instead,
     * access {@link #table} directly and check for {@code null} before writing.
     * </p>
     * 
     * @return the {@link #table} if this {@link Queue} is read optimized;
     *         otherwise, returns {@code null}.
     */
    @Nullable
    private Table table() {
        if(isReadOptimized()) {
            if(table == null) {
                table = new Table();
                for (Write write : writes) {
                    table.put(write);
                }
            }
            return table;
        }
        else {
            return null;
        }
    }

}