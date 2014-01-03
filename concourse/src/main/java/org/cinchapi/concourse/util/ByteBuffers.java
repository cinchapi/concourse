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
package org.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Additional utility methods for ByteBuffers that are not found in the
 * {@link ByteBuffer} class.
 * 
 * @author jnelson
 */
public abstract class ByteBuffers {

    /**
     * Return a ByteBuffer that is a new read-only buffer that shares the
     * content of {@code source} and has the same byte order, but maintains a
     * distinct position, mark and limit.
     * 
     * @param source
     * @return the new, read-only byte buffer
     */
    public static ByteBuffer asReadOnlyBuffer(ByteBuffer source) {
        int position = source.position();
        source.rewind();
        ByteBuffer duplicate = source.asReadOnlyBuffer();
        duplicate.order(source.order()); // byte order is not natively preserved
                                         // when making duplicates:
                                         // http://blog.mustardgrain.com/2008/04/04/bytebufferduplicate-does-not-preserve-byte-order/
        source.position(position);
        duplicate.rewind();
        return duplicate;
    }

    /**
     * Return a clone of {@code buffer} that has a copy of <em>all</em> its
     * content and the same position and limit. Unlike the
     * {@link ByteBuffer#slice()} method, the returned clone
     * <strong>does not</strong> share its content with {@code buffer}, so
     * subsequent operations to {@code buffer} or its clone will be
     * completely independent and won't affect the other.
     * 
     * @param buffer
     * @return a clone of {@code buffer}
     */
    public static ByteBuffer clone(ByteBuffer buffer) {
        ByteBuffer clone = ByteBuffer.allocate(buffer.capacity());
        int position = buffer.position();
        int limit = buffer.limit();
        buffer.rewind();
        clone.put(buffer);
        buffer.position(position);
        clone.position(position);
        buffer.limit(limit);
        clone.limit(limit);
        return clone;
    }

    /**
     * Encode the remaining bytes in as {@link ByteBuffer} as a hex string and
     * maintain the current position.
     * 
     * @param buffer
     * @return the hex string
     */
    public static String encodeAsHexString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        buffer.mark();
        while (buffer.hasRemaining()) {
            sb.append(String.format("%02x", buffer.get()));
        }
        buffer.reset();
        return sb.toString();
    }

    /**
     * Relative <em>get</em> method. Reads the byte at the current position in
     * {@code buffer} as a boolean, and then increments the position.
     * 
     * @param buffer
     * @return the boolean value at the current position
     * @see {@link ByteBufferOutputStream#write(boolean)}
     */
    public static boolean getBoolean(ByteBuffer buffer) {
        return buffer.get() > 0 ? true : false;
    }

    /**
     * Return a ByteBuffer that has a copy of {@code length} bytes from
     * {@code buffer} starting from the current position. This method will
     * advance the position of the source buffer.
     * 
     * @param buffer
     * @param bytes
     * @return a ByteBuffer that has {@code length} bytes from {@code buffer}
     */
    public static ByteBuffer get(ByteBuffer buffer, int length) {
        Preconditions
                .checkArgument(buffer.remaining() >= length,
                        "The number of bytes remaining in the buffer cannot be less than length");
        byte[] backingArray = new byte[length];
        buffer.get(backingArray);
        return ByteBuffer.wrap(backingArray);
    }

    /**
     * Relative <em>get</em> method. Reads the enum at the current position in
     * {@code buffer} and then increments the position by four.
     * 
     * @param buffer
     * @param clazz
     * @return the enum value at the current position
     * @see {@link ByteBufferOutputStream#write(Enum)}
     */
    public static <T extends Enum<?>> T getEnum(ByteBuffer buffer,
            Class<T> clazz) {
        return clazz.getEnumConstants()[buffer.getInt()];
    }

    /**
     * Relative <em>get</em> method. Reads the UTF-8 encoded string at
     * the current position in {@code buffer}.
     * 
     * @param buffer
     * @param charset
     * @return the string value at the current position
     * @see {@link ByteBufferOutputStream#write(String)}
     */
    public static String getString(ByteBuffer buffer) {
        return getString(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Relative <em>get</em> method. Reads the {@code charset} encoded string at
     * the current position in {@code buffer}.
     * 
     * @param buffer
     * @param charset
     * @return the string value at the current position
     * @see {@link ByteBufferOutputStream#write(String, Charset)}
     */
    public static String getString(ByteBuffer buffer, Charset charset) {
        try {
            CharsetDecoder decoder = charset.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.IGNORE);
            return decoder.decode(buffer).toString().trim(); // it
                                                             // is
                                                             // necessary
                                                             // to
                                                             // trim
                                                             // here
                                                             // because
                                                             // the
                                                             // decoding
                                                             // picks
                                                             // up
                                                             // trailing
                                                             // whitespace
                                                             // sometimes
        }
        catch (CharacterCodingException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return a new ByteBuffer whose content is a shared subsequence of the
     * content in {@code buffer} starting at the current position to
     * current position + {@code length} (non-inclusive). Invoking this method
     * has the same affect as doing the following:
     * 
     * <pre>
     * buffer.mark();
     * int oldLimit = buffer.limit();
     * buffer.limit(buffer.position() + length);
     * 
     * ByteBuffer slice = buffer.slice();
     * 
     * buffer.reset();
     * buffer.limit(oldLimit);
     * </pre>
     * 
     * @param buffer
     * @param length
     * @return the new ByteBuffer slice
     * @see {@link ByteBuffer#slice()}
     */
    public static ByteBuffer slice(ByteBuffer buffer, int length) {
        return slice(buffer, buffer.position(), length);
    }

    /**
     * Return a new ByteBuffer whose content is a shared subsequence of the
     * content in {@code buffer} starting at {@code position} to
     * {@code position} + {@code length} (non-inclusive). Invoking this method
     * has the same affect as doing the following:
     * 
     * <pre>
     * buffer.mark();
     * int oldLimit = buffer.limit();
     * buffer.position(position);
     * buffer.limit(position + length);
     * 
     * ByteBuffer slice = buffer.slice();
     * 
     * buffer.reset();
     * buffer.limit(oldLimit);
     * </pre>
     * 
     * @param buffer
     * @param position
     * @param length
     * @return the new ByteBuffer slice
     * @see {@link ByteBuffer#slice()}
     */
    public static ByteBuffer slice(ByteBuffer buffer, int position, int length) {
        int oldPosition = buffer.position();
        int oldLimit = buffer.limit();
        buffer.position(position);
        buffer.limit(position + length);
        ByteBuffer slice = buffer.slice();
        buffer.limit(oldLimit);
        buffer.position(oldPosition);
        return slice;
    }

    /**
     * Return a byte array with the content of {@code buffer}. This method
     * returns the byte array that backs {@code buffer} if one exists, otherwise
     * it creates a new byte array with the content between the current position
     * of {@code buffer} and its limit.
     * 
     * @param buffer
     * @return the byte array with the content of {@code buffer}
     */
    public static byte[] toByteArray(ByteBuffer buffer) {
        if(buffer.hasArray()) {
            return buffer.array();
        }
        else {
            buffer.mark();
            byte[] array = new byte[buffer.remaining()];
            buffer.get(array);
            buffer.reset();
            return array;
        }
    }

}
