/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.io;

/**
 * A utility class for performing bitwise operations on bytes. This class
 * intentionally only operates on bytes so that is clear and concise. If it is
 * necessary to perform bitwise operations on larger numbers (i.e. shorts, ints,
 * longs), the caller should build custom functions that leverage these.
 * 
 * @author jnelson
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
