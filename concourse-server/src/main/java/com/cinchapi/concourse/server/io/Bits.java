/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.io;

/**
 * A utility class for performing bitwise operations on bytes. This class
 * intentionally only operates on bytes so that is clear and concise. If it is
 * necessary to perform bitwise operations on larger numbers (i.e. shorts, ints,
 * longs), the caller should build custom functions that leverage these.
 * 
 * @author Jeff Nelson
 */
public final class Bits {

    /**
     * Flip the {@code pos}th bit of {@code b}.
     * @param pos
     * @param b
     * 
     * @return the number that results from the bit flip
     */
    public static byte flip(byte pos, byte b) {
        return (byte) (b ^ (1 << pos));
    }

    /**
     * Return the bit (0 or 1) at the {@code pos}th position of {@code b}.
     * @param pos
     * @param b
     * 
     * @return the positioned bit
     */
    public static boolean get(byte pos, byte b) {
        return ((b >> pos) & 1) == 0 ? false : true;
    }

    /**
     * Return {@code true} if the {@code pos}th bit in {@code b} is turned on
     * (e.g equals 1).
     * 
     * @param pos
     * @param b
     * @return {@code true} if the bit is on
     */
    public static boolean isOn(byte pos, byte b) {
        return get(pos, b);
    }

    private Bits() {/* noop */}

}
