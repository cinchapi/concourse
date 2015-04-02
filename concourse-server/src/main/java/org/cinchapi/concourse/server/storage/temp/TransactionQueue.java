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

import org.cinchapi.concourse.server.storage.PermanentStore;
import org.cinchapi.concourse.server.storage.cache.BloomFilter;

/**
 * A special {@link Queue} that is used for {@link Transaction transactions}:
 * uses a local bloom filter to make verifies more efficient.
 * 
 * @author Jeff Nelson
 */
public class TransactionQueue extends Queue {

    /**
     * An empty array of writes that is used to specify type conversion in the
     * {@link ArrayList#toArray(Object[])} method.
     */
    private static final Write[] EMPTY_WRITES_ARRAY = new Write[0];

    /**
     * The bloom filter used to speed up verifies.
     */
    private final BloomFilter bloom = BloomFilter.create(500000);

    /**
     * Construct a new instance.
     * 
     * @param initialSize
     */
    public TransactionQueue(int initialSize) {
        super(initialSize);
    }

    @Override
    public boolean insert(Write write, boolean sync) {
        if(super.insert(write, sync)) {
            bloom.putCached(write.getKey(), write.getValue(), write.getRecord());
            return true;
        }
        else {
            return false;
        }
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
        if(bloom.mightContainCached(write.getKey(), write.getValue(),
                write.getRecord())) {
            return super.verify(write, timestamp, exists);
        }
        else {
            return exists;
        }
    }

}
