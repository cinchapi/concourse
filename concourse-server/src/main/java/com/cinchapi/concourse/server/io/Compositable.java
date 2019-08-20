/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.io;

import java.nio.ByteBuffer;

/**
 * A {@link Compositable} object can be used to create a {@link Composite} using
 * "essential information" about the object. The binary form of a
 * {@link Compositable} is known as its "canonical" set of bytes and can be
 * retrieved using the {@link #getCanonicalBytes()} method.
 * <p>
 * {@link Compositable} objects have a notion of <strong>essential
 * information</strong> that is generally a subset of all the object's
 * information and is the minimum information required to reconstruct the object
 * <em>or an {@link #equals(Object) equivalent} one.</em> So, if two
 * {@link Compositable} Objects are logically different, but essentially
 * "equal", both objects should have the same {@link #getCanonicalBytes()
 * canonical bytes} and affect the make up of a {@link Composite} in the same
 * way.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Compositable {

    /**
     * Copy the essential {@link #getCanonicalBytes() bytes } from this
     * {@link Compositable} to the {@link ByteBuffer buffer}.
     * 
     * <p>
     * <strong>DO NOT</strong> make any modifications to buffer other than
     * filling it with bytes for this class (i.e. do not rewind the buffer or
     * change its position). It is assumed that the caller will rewind or flip
     * the buffer after this method completes.
     * </p>
     * 
     * @param buffer
     */
    public void copyCanonicalBytesTo(ByteBuffer buffer);

    /**
     * Copy the essential {@link #getCanonicalBytes() bytes} from this
     * {@link Compositable} to the {@link ByteSink sink}.
     * 
     * @param sink
     */
    public void copyCanonicalBytesTo(ByteSink sink);

    /**
     * Return a {@link ByteBuffer} containing a byte sequence that encodes the
     * essential information about this object.
     * 
     * @return the essential {@link ByteBuffer bytes}
     */
    public ByteBuffer getCanonicalBytes();
    
    /**
     * Return the number of {@link #getCanonicalBytes() bytes} needed to encode
     * essential information about this object.
     * 
     * @return the {@link #getCanonicalBytes()} size
     */
    public int getCanonicalLength();

}
