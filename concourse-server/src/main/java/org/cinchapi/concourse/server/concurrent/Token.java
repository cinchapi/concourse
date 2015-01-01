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
package org.cinchapi.concourse.server.concurrent;

import java.nio.ByteBuffer;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.storage.cache.LazyCache;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.concourse.util.TArrays;

import com.google.common.io.BaseEncoding;

/**
 * A {@link Token} wraps multiple other objects.
 * 
 * @author jnelson
 */
public class Token implements Byteable {

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
     * Return a {@link Token} that wraps the specified {@code objects}.
     * 
     * @param objects
     * @return the Token
     */
    public static Token wrap(Object... objects) {
        return new Token(TArrays.hash(objects));
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
     * The cache of string tokens that represent record keys.
     */
    private static final LazyCache<String, Token> cache = LazyCache
            .withExpectedSize(5000);

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
                .encode(ByteBuffers.toByteArray(getBytes())).toLowerCase();
    }

    @Override
    public void copyTo(ByteBuffer buffer) {
        ByteBuffers.copyAndRewindSource(bytes, buffer);
    }

}
