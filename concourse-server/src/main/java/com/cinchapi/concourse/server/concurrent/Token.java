/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.concourse.server.concurrent;

import java.nio.ByteBuffer;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.ByteSinks;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.storage.cache.LazyCache;
import com.cinchapi.concourse.util.TArrays;
import com.google.common.io.BaseEncoding;

/**
 * A {@link Token} wraps multiple other objects.
 * 
 * @author Jeff Nelson
 */
public class Token implements Byteable {

    /**
     * The cache of string tokens that represent record keys.
     */
    private static final LazyCache<String, Token> cache = LazyCache
            .withExpectedSize(5000);

    /**
     * Return the Token encoded in {@code bytes} so long as those bytes adhere
     * to the format specified by the {@link #getBytes()} method. This method
     * assumes that all the bytes in the {@code bytes} belong to the Token. In
     * general, it is necessary to get the appropriate Token slice from the
     * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param bytes
     * @return the Token
     */
    public static Token fromByteBuffer(ByteBuffer bytes) {
        return new Token(bytes);
    }

    /**
     * Return a {@link Token} that wraps the specified {@code key} and can be
     * exchanged for a {@link SharedReadWriteLock} that provides more flexible
     * concurrency controls.
     * 
     * @param object
     * @return the {@link Token}
     */
    public static Token shareable(long object) {
        Token token = new SharedToken(TArrays.hash(object));
        return token;
    }

    /**
     * Return a {@link Token} that wraps the specified {@code objects}.
     * 
     * @param objects
     * @return the Token
     */
    public static Token wrap(Object... objects) {
        Token token = new Token(TArrays.hash(objects));
        return token;
    }

    /**
     * Return a {@link Token} that wraps the specified {@code key}. This method
     * takes advantage of caching since the keys are often to be reused
     * frequently.
     * 
     * @param key
     * @return the Token
     */
    public static Token wrap(String key) {
        Token token = cache.get(key);
        if(token == null) {
            token = new Token(TArrays.hash(key));
            cache.put(key, token);
        }
        return token;
    }

    /**
     * The sequence of bytes is a 128-bit (16 byte) hash.
     */
    private final ByteBuffer bytes;

    /**
     * Construct a new instance.
     * 
     * @param bytes
     */
    protected Token(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    @Override
    public void copyTo(ByteSink sink) {
        ByteSinks.copyAndRewindSource(bytes, sink);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Token) {
            return getBytes().equals(((Token) obj).getBytes());
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
        return BaseEncoding.base16()
                .encode(ByteBuffers.getByteArray(getBytes())).toLowerCase();
    }

}
