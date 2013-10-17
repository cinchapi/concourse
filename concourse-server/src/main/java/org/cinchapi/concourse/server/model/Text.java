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
package org.cinchapi.concourse.server.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.server.io.Byteable;
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
	 * Represents an empty text string.
	 */
	public static final Text EMPTY = Text.wrap("");

	/**
	 * The wrapped string.
	 */
	private final String text;

	/**
	 * Master byte sequence that represents this object. Read-only duplicates
	 * are made when returning from {@link #getBytes()}.
	 */
	private final transient ByteBuffer bytes;

	/**
	 * Construct an instance that wraps the {@code text} string.
	 * 
	 * @param text
	 */
	private Text(String text) {
		this(text, ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)));
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
		return ByteBuffers.asReadOnlyBuffer(bytes);
	}

	@Override
	public int hashCode() {
		return text.hashCode();
	}

	@Override
	public int size() {
		return bytes.capacity();
	}

	@Override
	public String toString() {
		return text;
	}

}
