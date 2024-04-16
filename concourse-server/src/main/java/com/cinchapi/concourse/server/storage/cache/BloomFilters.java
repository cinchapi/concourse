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
package com.cinchapi.concourse.server.storage.cache;

import com.google.common.base.Preconditions;

/**
 * Utilities for {@link BloomFilter BloomFilters}.
 *
 * @author Jeff Nelson
 */
public final class BloomFilters {

    /**
     * Return an estimate of the number of elements in the union of {@code a}
     * and {@code b}.
     * 
     * @param a
     * @param b
     * @return the estimated size of (a ⋃ b)
     */
    public static long estimateUnionCount(BloomFilter a, BloomFilter b) {
        Preconditions.checkArgument(a.source().isCompatible(b.source()),
                "The two bloom filters are not compatible");
        BloomFilter union = BloomFilter.load(a.getBytes());
        union.source().putAll(b.source());
        return union.source().approximateElementCount();
    }

    /**
     * Return an estimate of the number of elements in the intersection of
     * {@code a} and {@code b}.
     * 
     * @param a
     * @param b
     * @return the estimated size of (a ⋂ b)
     */
    public static long estimateIntersectionCount(BloomFilter a, BloomFilter b) {
        return (a.source().approximateElementCount()
                + b.source().approximateElementCount())
                - estimateUnionCount(a, b);
    }

    /**
     * Return an estimate Jaccard Index, which measures the similarity between
     * {@code a} and {@code b} on a scale between 0 and 1.
     * 
     * @param a
     * @param b
     * @return the estimated Jaccard Index
     */
    public static double estimateSimilarity(BloomFilter a, BloomFilter b) {
        return (double) estimateIntersectionCount(a, b)
                / (double) estimateUnionCount(a, b);
    }

    private BloomFilters() {/* no init */}

}
