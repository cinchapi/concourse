/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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
import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.util.ByteBuffers;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;

/**
 * A {@link Composite} is a single {@link Byteable} composed of other
 * {@link Byteable Byteables}.
 * <p>
 * A {@link Composite} is used to construct keys with multiple parts.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class Composite implements Byteable {

    /**
     * Create a new {@link Composite}.
     * 
     * @param parts
     * @return the new {@link Composite}
     */
    public static Composite create(Byteable... parts) {
        return new Composite(parts);
    }

    /**
     * Load an existing {@link Composite} from a {@link ByteBuffer}.
     * 
     * @param bytes
     * @return the loaded {@link Composite}
     */
    public static Composite load(ByteBuffer bytes) {
        return new Composite(bytes);
    }

    /**
     * Create a Composite for the list of {@code objects} with support for
     * caching. Cached Composites are not guaranteed to perfectly match up with
     * the list of objects (because hash collisions can occur) so it is only
     * advisable to use this method of creation when precision is not a
     * requirement.
     * 
     * @param objects
     * @return the Composite
     */
    public static Composite createCached(Byteable... objects) {
        int hashCode = Arrays.hashCode(objects);
        Composite composite = CACHE.get(hashCode);
        if(composite == null) {
            composite = create(objects);
            CACHE.put(hashCode, composite);
        }
        return composite;
    }

    /**
     * A cache of Composite. Each composite is associated with the cumulative
     * hashcode of all the things that went into the composite.
     */
    private final static Map<Integer, Composite> CACHE = Maps.newHashMap();

    /**
     * The composite bytes.
     */
    private final ByteBuffer bytes;

    /**
     * The input parts. This is generally not available when the
     * {@link Composite} is {@link #load(ByteBuffer) loaded}.
     */
    private final Byteable[] parts;

    /**
     * Construct a new instance.
     * 
     * @param parts
     */
    private Composite(Byteable... parts) {
        this.parts = parts;
        if(parts.length == 1) {
            bytes = parts[0].getCanonicalBytes();
        }
        else {
            int size = 0;
            for (Byteable part : parts) {
                size += part.getCanonicalLength();
            }
            bytes = ByteBuffer.allocate(size + (4 * parts.length));
            int pos = 0;
            for (Byteable part : parts) {
                bytes.putInt(pos);
                part.copyCanonicalBytesTo(ByteSink.to(bytes));
            }
            bytes.flip();
        }
    }

    /**
     * Load a new instance.
     * 
     * @param bytes
     */
    private Composite(ByteBuffer bytes) {
        this.bytes = bytes;
        this.parts = null;
    }

    @Override
    public void copyTo(ByteSink sink) {
        ByteSinks.copyAndRewindSource(bytes, sink);
    }

    @Override
    public ByteBuffer getBytes() {
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    /**
     * Return {@code true} if {@link #parts()} will return a non-null value.
     * 
     * @return {@code true} if this {@link Composite} is aware of its individual
     *         parts
     */
    public boolean hasParts() {
        return parts != null;
    }

    /**
     * Return the individual {@link Byteable parts} that make up this
     * {@link Composite}, if they're available. If the parts are not available,
     * return {@code null}
     * 
     * @return the {@link Composite} parts
     */
    @Nullable
    public Byteable[] parts() {
        return parts;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Composite) {
            Composite other = (Composite) obj;
            return bytes.equals(other.bytes);
        }
        return false;
    }

    @Override
    public int size() {
        return bytes.capacity();
    }

    @Override
    public String toString() {
        return Hashing.murmur3_128().hashBytes(getBytes()).toString();
    }

}
