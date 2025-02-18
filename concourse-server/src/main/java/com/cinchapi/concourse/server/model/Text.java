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
package com.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.server.io.ByteSink;
import com.cinchapi.concourse.server.io.Byteable;
import com.cinchapi.concourse.server.storage.cache.LazyCache;

/**
 * A {@link Byteable} wrapper for a string of UTF-8 encoded characters.
 * 
 * @author Jeff Nelson
 */
@Immutable
public abstract class Text implements Byteable, Comparable<Text> {

    /**
     * Return the {@link Text} encoded in {@code bytes} so long as those bytes
     * adhere to the format specified by the {@link #getBytes()} method.
     * 
     * @param bytes
     * @return the {@link Text}
     */
    public static Text fromByteBuffer(ByteBuffer bytes) {
        return new StringText(ByteBuffers.getUtf8String(bytes));
    }

    /**
     * Return the {@link Text} encoded in {@code bytes} so long as those bytes
     * adhere to the format specified by the {@link #getBytes()} method. This
     * method assumes that all the bytes in the {@code bytes} belong to the
     * Text. In general, it is necessary to get the appropriate Text slice from
     * the parent ByteBuffer using
     * {@link ByteBuffers#slice(ByteBuffer, int, int)}.
     * <p>
     * It is possible that
     * the object will be a cached instance. This should only be called when
     * loading record keys since they are expected to be used often.
     * </p>
     * 
     * @param bytes
     * @return the {@link Text}
     */
    public static Text fromByteBufferCached(ByteBuffer bytes) {
        String value = ByteBuffers.getUtf8String(bytes);
        Text text = cache.get(value);
        if(text == null) {
            text = new StringText(value);
            cache.put(value, text);
        }
        return text;
    }

    /**
     * Return {@link Text} that is backed by a char array, beginning at index
     * {@code start} and ending at index {@code end} (non-inclusive).
     * <p>
     * It is assumed that {@code chars} is immutable.
     * </p>
     * 
     * @param chars
     * @param start
     * @param end
     * @return the {@link Text}
     */
    public static Text wrap(char[] chars, int start, int end) {
        return new CharText(chars, start, end);
    }

    /**
     * Return {@link Text} that is backed by {@code string}.
     * 
     * @param string
     * @return the {@link Text}
     */
    public static Text wrap(String string) {
        return new StringText(string);
    }

    /**
     * Return {@link Text} that is backed by {@code string}. It is possible that
     * the object will be a cached instance. This should only be called when
     * wrapping record keys since they are expected to be used often.
     * 
     * @param string
     * @return the {@link Text}
     */
    public static Text wrapCached(String string) {
        Text text = cache.get(string);
        if(text == null) {
            text = new StringText(string);
            cache.put(string, text);
        }
        return text;
    }

    /**
     * Represents an empty text string.
     */
    public static final Text EMPTY = Text.wrap("");

    /**
     * The cache that holds the objects created from the
     * {@link #wrapCached(String)} method. This is primary used for string
     * keys since those are expected to be used often.
     */
    private static final LazyCache<String, Text> cache = LazyCache
            .withExpectedSize(5000);

    /**
     * Return a more space optimized version of this {@link Text} with the same
     * value that is guaranteed to return {@code true} from
     * {@link #isCompact()}.
     * 
     * @return a compact version of this {@link Text}
     */
    public abstract Text compact();

    @Override
    public int compareTo(Text o) {
        // This implementation is adapted from java.lang.String#compareTo
        int length1 = length();
        int length2 = o.length();
        int limit = Math.min(length1, length2);
        for (int i = 0; i < limit; ++i) {
            char c1 = charAt(i);
            char c2 = o.charAt(i);
            if(c1 != c2) {
                return c1 - c2;
            }
        }
        return length1 - length2;
    }

    @Override
    public boolean equals(Object obj) {
        // This implementation is adapted from java.lang.String#equals
        if(this == obj) {
            return true;
        }
        else if(obj instanceof Text) {
            Text other = (Text) obj;
            int length = length();
            if(length == other.length()) {
                for (int i = 0; i < length; ++i) {
                    char c1 = charAt(i);
                    char c2 = other.charAt(i);
                    if(c1 != c2) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // This implementation is adapted from java.lang.String#hashCode
        int length = length();
        int h = 0;
        for (int i = 0; i < length; ++i) {
            char c = charAt(i);
            h = 31 * h + c;
        }
        return h;
    }

    /**
     * Return {@code true} if the internal representation of this {@link Text}
     * is space optimized. The particular space optimizations are undefined, but
     * this method can be used to introspect the {@link Text} and determine if
     * other optimizations need to be made in places where {@link Text} is used.
     * 
     * @return a boolean that indicates if this {@link Text} is space optimized
     */
    public abstract boolean isCompact();

    /**
     * Return {@code true} if there are no characters in this {@link Text}.
     * 
     * @return {@code true} if the {@link Text} is empty
     */
    public boolean isEmpty() {
        return length() == 0;
    }

    /**
     * Return the length of this {@link Text}.
     * 
     * @return the length
     */
    public abstract int length();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int length = length();
        for (int i = 0; i < length; ++i) {
            sb.append(charAt(i));
        }
        return sb.toString();
    }

    /**
     * Return {@link Text} with the same value as this one without any leading
     * or trailing whitespace.
     * 
     * @return trimmed {@link Text}
     */
    public abstract Text trim();

    /**
     * Return the character at position {@code index}. If {@code index} exceeds
     * the {@link #length()} of this {@link Text}, a
     * {@link IndexOutOfBounds} exception is thrown.
     * 
     * @param index
     * @return the character at position {@code index}
     * @throws IndexOutOfBoundsException
     */
    protected abstract char charAt(int index);

    /**
     * An implementation of {@link Text} whose value is a subsequence of a
     * char array.
     * <p>
     * {@link CharText} can be used for memory optimization by sharing a char
     * array between instances. Furthermore, speed optimizations that are made
     * in {@link StringText} (e.g. caching the generated {@link #getBytes() byte
     * representation}) are intentionally discarded so that minimal space is
     * maintained on the heap.
     * </p>
     *
     * @author Jeff Nelson
     */
    private static final class CharText extends Text {

        /**
         * The backing char array.
         */
        private char[] chars;

        /**
         * The last index (non-inclusive) of {@link #chars} that makes up the
         * value of this {@link Text}.
         */
        private int end;

        /**
         * The first index (inclusive) of {@link #chars} that makes up the value
         * of this {@link Text}.
         */
        private int start;

        /**
         * Construct a new instance.
         * 
         * @param chars
         * @param start
         * @param end
         */
        private CharText(char[] chars, int start, int end) {
            Verify.thatArgument(start >= 0);
            Verify.thatArgument(end >= start);
            Verify.thatArgument(end <= chars.length);
            this.chars = chars;
            this.start = start;
            this.end = end;
        }

        @Override
        public Text compact() {
            return this;
        }

        @Override
        public void copyTo(ByteSink sink) {
            sink.put(getBytes());
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof CharText) {
                CharText other = (CharText) obj;
                if(chars == other.chars && start == other.start
                        && end == other.end) {
                    return true;
                }
            }
            return super.equals(obj);
        }

        @Override
        public ByteBuffer getBytes() {
            CharBuffer buffer = CharBuffer.wrap(chars, start, length());
            return StandardCharsets.UTF_8.encode(buffer);
        }

        @Override
        public boolean isCompact() {
            return true;
        }

        @Override
        public int length() {
            return end - start;
        }

        @Override
        public final int size() {
            return getBytes().remaining();
        }

        @Override
        public Text trim() {
            if(chars[start] == ' ' || chars[end - 1] == ' ') {
                int start = this.start;
                int end = this.end;
                while (start < end && chars[start] == ' ') {
                    ++start;
                }
                while (end > start && chars[end - 1] == ' ') {
                    --end;
                }
                return new CharText(chars, start, end);
            }
            else {
                return this;
            }
        }

        @Override
        protected char charAt(int index) {
            int position = start + index;
            if(index >= 0 && position < end) {
                return chars[position];
            }
            else {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    /**
     * An implementation of {@link Text} that is backed by a {@link String}.
     *
     * @author Jeff Nelson
     */
    private static final class StringText extends Text {

        /**
         * The wrapped string.
         */
        protected final String value;

        /**
         * Master byte sequence that represents this object. Read-only
         * duplicates are made when returning from {@link #getBytes()}.
         */
        @Nullable
        private transient ByteBuffer bytes = null;

        /**
         * A mutex used to synchronized the lazy setting of the byte buffer.
         */
        @Nullable
        private transient Object mutex = new Object();

        /**
         * Construct an instance that wraps the {@code text} string.
         * 
         * @param value
         */
        private StringText(String value) {
            this.value = value;
            this.bytes = null;
        }

        @Override
        public Text compact() {
            return Text.wrap(value.toCharArray(), 0, value.length());
        }

        @Override
        public int compareTo(Text o) {
            if(o instanceof StringText) {
                return value.compareTo(((StringText) o).value);
            }
            else {
                return super.compareTo(o);
            }
        }

        @Override
        public void copyTo(ByteSink sink) {
            if(bytes == null) {
                sink.putUtf8(value);
            }
            else {
                sink.put(getBytes());
            }
        }

        @Override
        public ByteBuffer getBytes() {
            if(bytes == null) {
                synchronized (mutex) {
                    if(bytes == null) { // must check again to prevent duplicate
                                        // copy if there is a race condition
                                        // with a cached instance
                        bytes = ByteBuffers.fromUtf8String(value);
                    }
                }
            }
            return ByteBuffers.asReadOnlyBuffer(bytes);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean isCompact() {
            return false;
        }

        @Override
        public int length() {
            return value.length();
        }

        @Override
        public final int size() {
            return bytes == null ? getBytes().remaining() : bytes.remaining();
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public Text trim() {
            String trimmed = value.trim();
            if(trimmed.length() != value.length()) {
                return new StringText(trimmed);
            }
            else {
                return this;
            }
        }

        @Override
        protected char charAt(int index) {
            return value.charAt(index);
        }
    }

}
