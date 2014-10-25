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
package org.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
import org.cinchapi.concourse.server.storage.cache.LazyCache;
import org.cinchapi.concourse.util.ByteBuffers;

/**
 * A {@link Byteable} wrapper for a string of UTF-8 encoded characters.
 * 
 * @author jnelson
 */
@Immutable
public final class Text implements Byteable, Comparable<Text> {

    /**
     * Return the Text encoded in {@code bytes} so long as those bytes adhere
     * to the format specified by the {@link #getBytes()} method. This method
     * assumes that all the bytes in the {@code bytes} belong to the Text. In
     * general, it is necessary to get the appropriate Text slice from the
     * parent ByteBuffer using {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * 
     * @param buffer
     * @return the Text
     */
    public static Text fromByteBuffer(ByteBuffer bytes) {
        return new Text(ByteBuffers.getString(bytes, StandardCharsets.UTF_8),
                bytes);
    }

    /**
     * Return Text that is backed by {@code string}.
     * 
     * @param string
     * @return the Text
     */
    public static Text wrap(String string) {
        return new Text(string);
    }

    /**
     * Return Text that is backed by {@code string}. It is possible that the
     * object will be a cached instance. This should only be called when
     * wrapping record keys since they are expected to be used often.
     * 
     * @param string
     * @return the Text
     */
    public static Text wrapCached(String string) {
        Text text = cache.get(string);
        if(text == null) {
            text = new Text(string);
            cache.put(string, text);
        }
        return text;
    }

    /**
     * The cache that holds the objects created from the
     * {@link #wrapCached(String)} method. This is primary used for string keys
     * since those are expected to be used often.
     */
    private static final LazyCache<String, Text> cache = LazyCache
            .withExpectedSize(5000);

    /**
     * Represents an empty text string.
     */
    public static final Text EMPTY = Text.wrap("");

    /**
     * Master byte sequence that represents this object. Read-only duplicates
     * are made when returning from {@link #getBytes()}.
     */
    private transient ByteBuffer bytes = null;

    /**
     * The wrapped string.
     */
    private final String text;

    /**
     * A mutex used to synchronized the lazy setting of the byte buffer.
     */
    private final Object mutex = new Object();

    /**
     * Construct an instance that wraps the {@code text} string.
     * 
     * @param text
     */
    private Text(String text) {
        this(text, null);
    }

    /**
     * Construct a new instance.
     * 
     * @param text
     * @param bytes
     */
    private Text(String text, @Nullable ByteBuffer bytes) {
        this.text = text;
        this.bytes = bytes;
    }

    @Override
    public int compareTo(Text o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Text) {
            Text other = (Text) obj;
            return toString().equals(other.toString());
        }
        return false;
    }

    @Override
    public ByteBuffer getBytes() {
        if(bytes == null) {
            synchronized (mutex) {
                if(bytes == null) { // must check again to prevent duplicate
                                    // copy if there is a race condition
                    bytes = ByteBuffers.fromString(text);
                }
            }
        }
        return ByteBuffers.asReadOnlyBuffer(bytes);
    }

    @Override
    public int hashCode() {
        return text.hashCode();
    }

    @Override
    public int size() {
        return bytes == null ? getBytes().capacity() : bytes.capacity();
    }

    @Override
    public String toString() {
        return text;
    }

    @Override
    public void copyToByteBuffer(ByteBuffer buffer) {
        if(bytes == null) {
            ByteBuffers.putString(text, buffer);
        }
        else {
            buffer.put(getBytes());
        }
    }

}
