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
package com.cinchapi.concourse.util;

import java.nio.ByteBuffer;

/**
 * Utilities for dealing with generic arrays.
 * 
 * @author Jeff Nelson
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
