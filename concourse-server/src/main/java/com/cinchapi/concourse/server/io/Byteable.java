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

import java.nio.ByteBuffer;

/**
 * An Byteable object can encode its representation as a sequence of bytes for
 * storage and transmission.
 * <p>
 * This interface is designed to be a lightweight replacement for java
 * serialization, externalization and other third party translation schemes with
 * the following advantages:
 * <ul>
 * <li>The implementing class has complete control over its serialization
 * format.</li>
 * <li>Objects can be serialized faster since java reflection is avoided.</li>
 * <li>No reliance on external dependencies (i.e. declaring the class schema or
 * serialization format in a separate file).</li>
 * <li>No requirement is imposed on superclasses (i.e. a superclass does not
 * need to also implement this interface or define a certain constructor for
 * compatibility).</li>
 * </ul>
 * </p>
 * <h2>Serialization</h2>
 * <p>
 * Write the bytes returned by {@link #getBytes()} to a file, stream, etc.
 * </p>
 * <h2>Deserialization</h2>
 * <p>
 * The implementing class must have a constructor that takes a ByteBuffer and
 * reconstructs the object by reading the bytes in the order of the scheme
 * defined in {@link #getBytes()}.
 * </p>
 * 
 * @see Byteables
 * @author Jeff Nelson
 */
public interface Byteable {

    /**
     * Copy the byte sequence that represents this object to the {@code buffer}.
     * This method should be idempotent, so if the object caches its byte
     * representation, be sure to reset the position after copying data to the
     * buffer.
     * <p>
     * This method is primary intended for pass-through gathering where data
     * from multiple Byteables can be copied to a single bytebuffer without
     * doing any unnecessary intermediate copying. So, if the binary
     * representation for this object depends on that of another Byteable, then
     * the implementation of this method should gather those bytes using the
     * {@link #copyTo(ByteBuffer)} method for the other Byteable.
     * </p>
     * <p>
     * <strong>DO NOT</strong> make any modifications to {@code buffer} other
     * than filling it with bytes for this class (i.e. do not rewind the buffer
     * or change its position). It is assumed that the caller will rewind or
     * flip the buffer after this method completes.
     * </p>
     * 
     * @param buffer
     */
    public void copyTo(ByteBuffer buffer);

    /**
     * Returns a byte sequence that represents this object.
     * 
     * @return the byte sequence.
     */
    public ByteBuffer getBytes();

    /**
     * Returns the total number of bytes used to represent this object.
     * <p>
     * It is recommended that the value returned from this method NOT depend on
     * a call to {@link #getBytes()}. Therefore, the implementing class should
     * keep track of the size separately, if necessary.
     * <p>
     * 
     * @return the number of bytes.
     */
    public int size();

}
