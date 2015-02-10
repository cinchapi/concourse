/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
 * @see {@link #Byteables}
 * @author jnelson
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
<<<<<<< HEAD
     * {@link #copyTo(ByteBuffer)} method for the other Byteable.
=======
     * {@link #copyToByteBuffer(ByteBuffer)} method for the other Byteable.
>>>>>>> de8748264fd8f0370664c027005cdaf90ba95252
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
<<<<<<< HEAD
    public void copyTo(ByteBuffer buffer);
=======
    public void copyToByteBuffer(ByteBuffer buffer);
>>>>>>> de8748264fd8f0370664c027005cdaf90ba95252

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
