/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage.temp;

import org.cinchapi.concourse.server.storage.cache.BloomFilter;

/**
 * A special {@link Queue} that is used for {@link Transaction transactions}:
 * uses a local bloom filter to make verifies more efficient.
 * 
 * @author jnelson
 */
public class TransactionQueue extends Queue {

    /**
     * The bloom filter used to speed up verifies.
     */
    private static final BloomFilter bloom = BloomFilter.create(500000);

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
