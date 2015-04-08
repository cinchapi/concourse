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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
     * Revisions are stored as a sequential list of {@link Write} objects, which
     * means most reads are <em>at least</em> an O(n) scan.
     */
    protected final List<Write> writes;

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
        return Collections.unmodifiableList(writes);
    }

    @Override
    public boolean insert(Write write, boolean sync) {
        return writes.add(write);// NOTE: #sync is
                                 // meaningless since
                                 // Queue is a memory
                                 // store
    }

    @Override
    public Iterator<Write> iterator() {
        return writes.iterator();
    }

    @Override
    public Iterator<Write> reverseIterator() {
        List<Write> copy = Lists.newArrayList(writes);
        Collections.reverse(copy);
        return copy.iterator();
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

}
