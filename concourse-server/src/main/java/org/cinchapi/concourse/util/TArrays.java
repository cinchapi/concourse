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
package org.cinchapi.concourse.util;

import java.nio.ByteBuffer;

/**
 * Utilities for dealing with generic arrays.
 * 
 * @author jnelson
 */
public final class TArrays {

    /**
     * Convert an array of objects (possibly of mixed types) to a ByteBuffer.
     * This method, in some sense, is akin to returning a hash of the objects,
     * except the size of the returned buffer will vary depending upon the
     * number of objects that are in the array.
     * <p>
     * This method assumes that the hashCode of the objects in the array are
     * defined in a manner that will yield the same value between JVM sessions.
     * There are no guarantees made about the content of returned ByteBuffer
     * except that objects that are considered equal will always returned byte
     * buffers that are equal.
     * </p>
     * 
     * @param objects
     * @return the hash for the objects
     */
    public static ByteBuffer hash(Object... objects) {
        ByteBuffer bytes = ByteBuffer.allocate(8 * objects.length);
        for (Object object : objects) {
            // We make a best effort to "uniquely" identify each object in
            // the array by that object's hashcode and the name of the
            // object's class.
            bytes.putInt(object.hashCode());
            bytes.putInt(object.getClass().getName().hashCode());
        }
        bytes.rewind();
        return bytes;
    }

}
