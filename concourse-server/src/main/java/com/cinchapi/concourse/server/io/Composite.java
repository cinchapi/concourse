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
import java.util.Arrays;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.annotate.DoNotInvoke;
import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;

/**
 * A {@link Composite} is a single Byteable object that wraps multiple other
 * Byteable objects.
 * 
 * @author Jeff Nelson
 */
@Immutable
public final class Composite implements Byteable {

    /**
     * Return a Composite for the list of {@code byteables}.
     * 
     * @param byteables
     * @return the Composite
     */
    public static Composite create(Byteable... byteables) {
        return new Composite(byteables);
    }

    /**
     * Create a Composite for the list of {@code byteables} with support for
     * caching. Cached Composites are not guaranteed to perfectly match up with
     * the list of byteables (because hash collisions can occur) so it is only
     * advisable to use this method of creation when precision is not a
     * requirement.
     * 
     * @param byteables
     * @return the Composite
     */
    public static Composite createCached(Byteable... byteables) {
        int hashCode = Arrays.hashCode(byteables);
        Composite composite = CACHE.get(hashCode);
        if(composite == null) {
            composite = create(byteables);
            CACHE.put(hashCode, composite);
        }
        return composite;
    }

    /**
     * Return the Token encoded in {@code bytes} so long as those
     * bytes adhere to the format specified by the {@link #getBytes()} method.
     * This method assumes that all the bytes in the {@code bytes} belong to the
     * Token. In general, it is necessary to get the appropriate
     * Value slice from the parent ByteBuffer using
     * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the Token
     */
    public static Composite fromByteBuffer(ByteBuffer bytes) {
        return Byteables.read(bytes, Composite.class);
    }

    /**
     * A cache of Composite. Each composite is associated with the cumulative
     * hashcode of all the things that went into the composite.
     */
    private final static Map<Integer, Composite> CACHE = Maps.newHashMap();

    private final ByteBuffer bytes;

    /**
     * Construct an instance that represents an existing Token from
     * a ByteBuffer. This constructor is public so as to comply with the
     * {@link Byteable} interface. Calling this constructor directly is not
     * recommend. Use {@link #fromByteBuffer(ByteBuffer)} instead to take
     * advantage of reference caching.
     * 
     * @param bytes
     */
    @DoNotInvoke
    public Composite(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    /**
     * Construct a new instance.
     * 
     * @param byteables
     */
    private Composite(Byteable... byteables) {
        if(byteables.length == 1) {
            bytes = byteables[0].getBytes();
        }
        else {
            int size = 0;
            for (Byteable byteable : byteables) {
                size += byteable.size();
            }
            bytes = ByteBuffer.allocate(size);
            for (Byteable byteable : byteables) {
                byteable.copyTo(bytes);
            }
            bytes.rewind();
        }
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        ByteBuffers.copyAndRewindSource(bytes, buffer);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Composite) {
            Composite other = (Composite) obj;
            return getBytes().equals(other.getBytes());
        }
        return false;
    }

    @Override
    public ByteBuffer getBytes() {
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    @Override
    public int hashCode() {
        return getBytes().hashCode();
    }

    @Override
    public int size() {
        return bytes.capacity();
    }

    @Override
    public String toString() {
        return Hashing.sha1().hashBytes(ByteBuffers.toByteArray(getBytes()))
                .toString();
    }

}
