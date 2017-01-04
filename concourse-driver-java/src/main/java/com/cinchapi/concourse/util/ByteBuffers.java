/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;

/**
 * Additional utility methods for ByteBuffers that are not found in the
 * {@link ByteBuffer} class.
 * 
 * @author Jeff Nelson
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
     * Transfer the bytes from {@code source} to {@code destination} and resets
     * {@code source} so that its position remains unchanged. The position of
     * the {@code destination} is incremented by the number of bytes that are
     * transferred.
     * 
     * @param source
     * @param destination
     */
    public static void copyAndRewindSource(ByteBuffer source,
            ByteBuffer destination) {
        int position = source.position();
        destination.put(source);
        source.position(position);
    }

    /**
     * Decode the {@code hex}adeciaml string and return the resulting binary
     * data.
     * 
     * @param hex
     * @return the binary data
     */
    public static ByteBuffer decodeFromHex(String hex) {
        return ByteBuffer.wrap(BaseEncoding.base16().decode(hex));
    }

    /**
     * Encode the {@code bytes} as a hexadecimal string.
     * 
     * @param bytes
     * @return the hex string
     */
    public static String encodeAsHex(ByteBuffer bytes) {
        bytes.rewind();
        return BaseEncoding.base16().encode(ByteBuffers.toByteArray(bytes));
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
     * Copy the remaining bytes in the {@code source} buffer to the
     * {@code destination}, expanding if necessary in
     * order to accommodate the bytes from {@code source}.
     * 
     * <p>
     * <strong>NOTE:</strong> This method may modify the {@code limit} for the
     * destination buffer.
     * </p>
     * 
     * @param destination the buffer into which the {@code source} is copied
     * @param source the buffer that is copied into the {@code destination}
     * @return a possibly expanded copy of {@code destination} with the
     *         {@code source} bytes copied
     */
    public static ByteBuffer expand(ByteBuffer destination, ByteBuffer source) {
        destination = ensureRemainingCapacity(destination, source.remaining());
        int newLimit = destination.position() + source.remaining();
        if(destination.limit() < newLimit) {
            destination.limit(newLimit);
        }
        destination.put(source);
        return destination;
    }

    /**
     * Put the {@code value} into {@code destination}, expanding if necessary in
     * order to accommodate the new bytes.
     * 
     * <p>
     * <strong>NOTE:</strong> This method may modify the {@code limit} for the
     * destination buffer.
     * </p>
     * 
     * @param destination the buffer into which the {@code source} is copied
     * @param value the value to add to the {@code destination}
     * @return a possibly expanded copy of {@code destination} with the
     *         {@code value} bytes copied
     */
    public static ByteBuffer expandInt(ByteBuffer destination, int value) {
        destination = ensureRemainingCapacity(destination, 4);
        int newLimit = destination.position() + 4;
        if(destination.limit() < newLimit) {
            destination.limit(newLimit);
        }
        destination.putInt(value);
        return destination;
    }

    /**
     * Return a byte buffer that has the UTF-8 encoding for {@code string}. This
     * method uses some optimization techniques and is the preferable way to
     * convert strings to byte buffers than doing so manually.
     * 
     * @param string
     * @return the byte buffer with the {@code string} data.
     */
    public static ByteBuffer fromString(String string) {
        try {
            return ByteBuffer.wrap(string.getBytes(CHARSET));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return a ByteBuffer that has a copy of {@code length} bytes from
     * {@code buffer} starting from the current position. This method will
     * advance the position of the source buffer.
     * 
     * @param buffer
     * @param length
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
     * Relative <em>get</em> method. Reads the byte at the current position in
     * {@code buffer} as a boolean, and then increments the position.
     * 
     * @param buffer
     * @return the boolean value at the current position
     */
    public static boolean getBoolean(ByteBuffer buffer) {
        return buffer.get() > 0 ? true : false;
    }

    /**
     * Relative <em>get</em> method. Reads the enum at the current position in
     * {@code buffer} and then increments the position by four.
     * 
     * @param buffer
     * @param clazz
     * @return the enum value at the current position
     */
    public static <T extends Enum<?>> T getEnum(ByteBuffer buffer,
            Class<T> clazz) {
        return clazz.getEnumConstants()[buffer.getInt()];
    }

    /**
     * Return a ByteBuffer that has a copy of all the remaining bytes from
     * {@code buffer} starting from the current position. This method will
     * advance the position of the source buffer.
     * 
     * @param buffer the source buffer
     * @return a ByteBuffer that has the remaining bytes from {@code buffer}
     */
    public static ByteBuffer getRemaining(ByteBuffer buffer) {
        return get(buffer, buffer.remaining());
    }

    /**
     * Relative <em>get</em> method. Reads the UTF-8 encoded string at
     * the current position in {@code buffer}.
     * 
     * @param buffer
     * @return the string value at the current position
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
     */
    public static String getString(ByteBuffer buffer, Charset charset) {
        CharsetDecoder decoder = null;
        try {
            if(charset == StandardCharsets.UTF_8) {
                while (decoder == null) {
                    decoder = DECODERS.poll();
                }
            }
            else {
                decoder = charset.newDecoder();
            }
            decoder.onMalformedInput(CodingErrorAction.IGNORE);
            return decoder.decode(buffer).toString();
        }
        catch (CharacterCodingException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if(decoder != null && charset == StandardCharsets.UTF_8) {
                DECODERS.offer(decoder);
            }
        }
    }

    /**
     * Return a ByteBuffer that contains a single null byte.
     * 
     * @return a null byte buffer
     */
    public static ByteBuffer nullByteBuffer() {
        ByteBuffer nullByte = ByteBuffer.allocate(1);
        nullByte.put((byte) 0);
        nullByte.rewind();
        return nullByte;

    }

    /**
     * Put the UTF-8 encoding for the {@code source} string into the
     * {@code destination} byte buffer and increment the position by the length
     * of the strings byte sequence. This method uses some optimization
     * techniques and is the preferable way to add strings to byte buffers than
     * doing so manually.
     * 
     * @param source
     * @param destination
     */
    public static void putString(String source, ByteBuffer destination) {
        try {
            byte[] bytes = source.getBytes(CHARSET);
            destination.put(bytes);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * The exact same as {@link ByteBuffer#rewind()} except it returns a typed
     * {@link ByteBuffer} instead of a generic {@link Buffer}.
     * 
     * @param buffer
     * @return {@code buffer}
     */
    public static ByteBuffer rewind(ByteBuffer buffer) {
        buffer.rewind();
        return buffer;
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
     * @see ByteBuffer#slice()
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
     * @see ByteBuffer#slice()
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

    /**
     * Return a UTF-8 {@link CharBuffer} representation of the bytes in the
     * {@code buffer}.
     * 
     * @param buffer
     * @return the char buffer
     */
    public static CharBuffer toCharBuffer(ByteBuffer buffer) {
        return toCharBuffer(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Return a {@link CharBuffer} representation of the bytes in the
     * {@code buffer} encoded with the {@code charset}.
     * 
     * @param buffer
     * @param charset
     * @return the char buffer
     */
    public static CharBuffer toCharBuffer(ByteBuffer buffer, Charset charset) {
        buffer.mark();
        CharBuffer chars = charset.decode(buffer);
        buffer.reset();
        return chars;
    }

    /**
     * Ensure that {@code buffer} has {@code capacity} bytes
     * {@link ByteBuffer#remaining() remaining} and return either {@code buffer}
     * or a copy that has enough capacity.
     * 
     * @param buffer the buffer to check for remaining capacity
     * @param capacity the number of bytes required
     * @return a {@link ByteBuffer} with all the contents of {@code buffer} and
     *         enough remaining room for {@code capacity} bytes
     */
    private static ByteBuffer ensureRemainingCapacity(ByteBuffer buffer,
            int capacity) {
        if((buffer.capacity() - buffer.position()) < capacity) {
            ByteBuffer copy = ByteBuffer
                    .allocate(((buffer.capacity() + capacity) * 3) / 2 + 1);
            buffer.limit(buffer.position());
            buffer.rewind();
            copy.put(buffer);
            buffer = copy;
        }
        return buffer;
    }

    /**
     * The name of the Charset to use for encoding/decoding. We use the name
     * instead of the charset object because Java caches encoders when
     * referencing them by name, but creates a new encorder object when
     * referencing them by Charset object.
     */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /**
     * A collection of UTF-8 decoders that can be concurrently used. We use this
     * to avoid creating a new decoder every time we need to decode a string
     * while still allowing multi-threaded access.
     */
    private static final ConcurrentLinkedQueue<CharsetDecoder> DECODERS = new ConcurrentLinkedQueue<CharsetDecoder>();

    /**
     * The number of UTF-8 decoders to create for concurrent access.
     */
    private static final int NUM_DECODERS = 10;
    static {
        try {
            for (int i = 0; i < NUM_DECODERS; ++i) {
                DECODERS.add(StandardCharsets.UTF_8.newDecoder());
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
