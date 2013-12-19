/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
import java.util.Arrays;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.util.ByteBuffers;

import com.google.common.hash.Hashing;
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
        return new Token(ByteBuffer.wrap(Hashing.md5()
                .hashUnencodedChars(Arrays.toString(objects)).asBytes()));
    }

    /**
     * The sequence of bytes is a MD5 hash.
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

}
