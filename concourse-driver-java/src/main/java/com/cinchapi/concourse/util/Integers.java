/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

/**
 * A collection of integer related utility functions that are not found in the
 * {@link Integer} or {@link Ints} classes.
 * 
 * @author Jeff Nelson
 */
public final class Integers {

    /**
     * Given an integer {@code n}, round it up to the next power of 2.
     * <p>
     * Props for this technique goes to Sean Eron Anderson courtesy of <a
     * href="https://graphics.stanford.edu/~seander/bithacks.html"
     * >https://graphics.stanford.edu/~seander/bithacks.html</a>
     * </p>
     * 
     * @param n
     * @return the next power of 2 for {@code n}.
     */
    public static int nextPowerOfTwo(int n) {
        --n;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        ++n;
        return n;
    }

    /**
     * Return the integer casted average of all the {@code ints}.
     * 
     * @param ints
     * @return the average
     */
    public static int avg(int... ints) {
        int sum = 0;
        int length = ints.length;
        for (int i = 0; i < length; ++i) {
            sum += ints[i];
        }
        return sum / length;
    }

    private Integers() {/* noop */}

}
